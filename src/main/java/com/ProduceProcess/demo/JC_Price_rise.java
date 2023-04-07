package com.ProduceProcess.demo;

import com.JLC.demo.JLCAllData2Tidb;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * DzProduce   com.ProduceProcess.demo
 * 2023-03-2023/3/31   16:30
 *
 * @author : zhangmingyue
 * @description : Process Price_rise_fall table
 * @date : 2023/3/31 4:30 PM
 */
public class JC_Price_rise {
    public static void main(String[] args) throws IOException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger logger = Logger.getLogger("SpzsIndex");
        logger.setLevel(Level.ERROR);
        //   read from configuration file, get configuration
        Properties prop = new Properties();
        InputStream inputStream = JLCAllData2Tidb.class.getClassLoader().getResourceAsStream("application.properties");
        prop.load(inputStream);

        String tidbUrl_warehouse = prop.getProperty("tidb.url_warehouse");
        String tidbUrl_product = prop.getProperty("tidb.url_product");
        String tidbUser = prop.getProperty("tidb.user");
        String tidbPassword = prop.getProperty("tidb.password");

        String indexTable = "st_spzs_index";
        String dataTable = "st_spzs_data";
        String treeTable = "st_spzs_tree";
        String priceRiseFallTable = "price_rise_fall";

        SparkSession sparkSession = SparkSession.builder()
                .appName("JLCDataUnifiedFormat")
//                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "8g")
                .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
                .getOrCreate();
        //      get tmpView
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, indexTable).createOrReplaceTempView("index");
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, dataTable).createOrReplaceTempView("data");
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, treeTable).createOrReplaceTempView("tree");
        getTmpView(sparkSession);

        Dataset<Row> price_riseDF = sparkSession.sql(getSql());
//        price_riseDF.show();
        writeToTiDB(price_riseDF, tidbUrl_product, tidbUser, tidbPassword, priceRiseFallTable);
        sparkSession.stop();
    }

    //      Get tableView function
    private static Dataset<Row> getDF(SparkSession sparkSession, String url, String user, String password, String table) {
        return sparkSession.read()
                .format("jdbc")
                .option("url", url)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", table)
                .option("user", user)
                .option("password", password)
                .load().toDF();
    }

    //  Get tmpView function
    private static void getTmpView(SparkSession sparkSession) {
        //  Get attr column
        String jsonSchema = "struct<product:struct<attrName:string>,BelongsArea:struct<attrName:string>>";

        String rankTableSql = "WITH parsed_content AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "          IndicatorName,\n" +
                "           unified,\n" +
                "           from_json(content, '" + jsonSchema + "') AS parsedContent\n" +
                "    FROM index " +
               /*"where IndicatorCode in (select b.treeID from(select treeid from tree where treeID = '58256e0ce80c2431e8e5a107') a join tree b on b.pathId like concat('%',a.treeid, '%'))"+//线螺:58256e0ce80c2431e8e5a107 //甲醇:57c8f3cce80c19cd2f334c82 //大豆:100000003*/
                "where IndicatorCode in (select b.treeID from(select treeid from tree where treeID in ('58256e0ce80c2431e8e5a107','57c8f3cce80c19cd2f334c82', 'DD100000003DD')) a join tree b on b.pathId like concat('%',a.treeid, '%'))"+
                "),\n" +
                "tmp AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "           IndicatorName,\n" +
                "           unified,\n" +
                "           parsedContent.product.attrName AS product,\n" +
                "           parsedContent.BelongsArea.attrName AS BelongsArea\n" +
                "    FROM parsed_content \n" +
                "),\n" +
                "rank_Table AS (\n" +
                "    SELECT tmp.IndicatorCode,\n" +
                "           tmp.IndicatorName,\n" +
                "           tmp.unified,\n" +
                "           tmp.product,\n" +
                "           tmp.BelongsArea,\n" +
                "           data.pubDate,\n" +
                "           data.measureValue,\n" +
                "           ROW_NUMBER() OVER (PARTITION BY tmp.IndicatorCode ORDER BY data.pubDate DESC) AS row_num\n" +
                "    FROM tmp\n" +
                "    JOIN data ON tmp.IndicatorCode = data.IndicatorCode" +
                " where data.measureName != 'remark' \n" +
                ")\n" +
                "SELECT IndicatorCode,\n" +
                "           IndicatorName,\n" +
                "           unified,\n" +
                "           product,\n" +
                "           BelongsArea,\n" +
                "           pubDate,\n" +
                "           measureValue,\n" +
                "       row_num\n" +
                "FROM rank_Table ";
        sparkSession.sql(rankTableSql).createOrReplaceTempView("rank_Table");
//        sparkSession.sql(rankTableSql).show();
    }

    //  Return SQL query statement
    private static String getSql() {
        return "WITH latest_dates AS (\n" +
                "    SELECT IndicatorCode, pubDate as latest_date,measureValue as latest_measure_value\n" +
                "    FROM rank_table\n" +
                "    where row_num = 1\n" +
                "),\n" +
                "previous_year_dates AS (\n" +
                "    SELECT t1.IndicatorCode,\n" +
                "           t1.pubDate as previous_year_date,\n" +
                "           t1.measureValue as previous_year_measure_value\n" +
                "    FROM rank_table t1\n" +
                "             INNER JOIN latest_dates t2\n" +
                "                        ON t1.IndicatorCode = t2.IndicatorCode\n" +
                "    WHERE t1.pubDate = date_add(t2.latest_date, -365)\n" +
                "),\n" +
                "yoy_tabel as (SELECT t3.IndicatorCode,\n" +
                "                     t3.latest_date,\n" +
                "                     t4.previous_year_date,\n" +
                "                     t3.latest_measure_value,\n" +
                "                     t4.previous_year_measure_value,\n" +
                "                     (t3.latest_measure_value - t4.previous_year_measure_value) as measure_value_difference\n" +
                "              FROM latest_dates t3\n" +
                "                       INNER JOIN previous_year_dates t4\n" +
                "                                  ON t3.IndicatorCode = t4.IndicatorCode),\n" +
                "rise_fall_table as (select product                                         as product,\n" +
                "                           IndicatorCode                                   as indicator_code,\n" +
                "                           IndicatorName                                   as indicator_name,\n" +
                "                           BelongsArea                                     as address,\n" +
                "                           measureValue                                    as price,\n" +
                "                           previous_price                                  as previous_price,\n" +
                "                           (measureValue - previous_price)                 as rise_fall,\n" +
                "                           round((measureValue - previous_price) / COALESCE(NULLIF(previous_price, 0), 1) * 100, 6) as percentage,\n" +
                "                           pubDate                                         as `to_date`,\n" +
                "                           unified                                         as unit\n" +
                "                    from (select IndicatorCode,IndicatorName,unified,BelongsArea,measureValue,pubDate,product,row_num,\n" +
                "                                 LEAD(measureValue) OVER (PARTITION BY IndicatorCode ORDER BY pubDate desc) AS previous_price\n" +
                "                          from rank_table\n" +
                "                          WHERE measureValue is not null\n" +
                "                            and row_num <= 2) tmp1\n" +
                "                    where row_num = 1)\n" +
                "select\n" +
                "product,\n" +
                "indicator_code,\n" +
                "indicator_name,\n" +
                "address,\n" +
                "price,\n" +
                "previous_price,\n" +
                "rise_fall,\n" +
                "percentage,\n" +
                "to_date,\n" +
                "unit,\n" +
                "rise_fall as ring_ratio,\n" +
                "measure_value_difference as yoy\n" +
                "from rise_fall_table rft\n" +
                "         left join yoy_tabel yt\n" +
                "                   on rft.indicator_code = yt.IndicatorCode";
    }
    //  write to Tidb
    private static void writeToTiDB(Dataset<Row> dataFrame, String url, String user, String password, String table) {
        dataFrame.repartition(10).write()
                .mode(SaveMode.Append)
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", url)
                .option("user", user)
                .option("password", password)
                .option("dbtable", table)
                .option("isolationLevel", "NONE")    //不开启事务
                .option("batchsize", 10000)   //设置批量插入
                .save();
    }
}
