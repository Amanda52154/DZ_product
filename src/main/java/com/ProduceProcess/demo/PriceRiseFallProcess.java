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
 * 2023-03-2023/3/30   09:09
 *
 * @author : zhangmingyue
 * @description : Process price_rise_fall data
 * @date : 2023/3/30 9:09 AM
 */
public class PriceRiseFallProcess {
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
                .master("local[*]")
                .getOrCreate();
        //      get tmpView
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, indexTable).createOrReplaceTempView("index");
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, dataTable).createOrReplaceTempView("data");
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, treeTable).createOrReplaceTempView("tree");
        getTmpView(sparkSession);

        //      Process Price_rise_table data
        Dataset<Row> price_riseDF = sparkSession.sql(getSql());
        price_riseDF.show();
        writeToTiDB(price_riseDF, tidbUrl_product, tidbUser, tidbPassword, priceRiseFallTable);

        sparkSession.stop();
    }

    //   Get tableView function
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
        String jsonSchema = "struct<product:struct<attrName:string>,BelongsArea:struct<attrName:string>,measure:struct<attrName:string>,upd_freq:struct<attrName:string>,caliber:struct<attrName:string>>";

        /*String getContentSql = "select distinct IndicatorCode, content from index";
        Dataset<Row> contentData = sparkSession.sql(getContentSql);
        Dataset<Row> parsedData = contentData.selectExpr("IndicatorCode", "from_json(content, '" + jsonSchema + "') as parsedContent");
        parsedData.selectExpr("IndicatorCode", "parsedContent.product.attrName as product", "parsedContent.BelongsArea.attrName as BelongsArea", "parsedContent.measure.attrName as measure", "parsedContent.upd_freq.attrName as upd_freq", "parsedContent.caliber.attrName as caliber").createOrReplaceTempView("tmp");

        String ranke_tabel = "SELECT tmp1.IndicatorCode,\n" +
                "           tmp1.IndicatorName,\n" +
                "           tmp1.endDate,\n" +
                "           tmp1.upd_freq,\n" +
                "           tmp1.unified,\n" +
                "           tmp1.product,\n" +
                "           tmp1.BelongsArea,\n" +
                "           tmp1.measure,\n" +
                "           tmp1.caliber,\n" +
                "           data.pubDate,\n" +
                "           data.measureName,\n" +
                "           data.measureValue,\n" +
                "           ROW_NUMBER() OVER (PARTITION BY tmp1.IndicatorCode ORDER BY data.pubDate DESC) AS row_num\n" +
                "    FROM (\n" +
                "             SELECT index.IndicatorCode,\n" +
                "                    index.IndicatorName,\n" +
                "                    index.endDate,\n" +
                "                    index.upd_freq,\n" +
                "                    index.unified,\n" +
                "                    tmp.product,\n" +
                "                    tmp.BelongsArea,\n" +
                "                    tmp.measure,\n" +
                "                    tmp.caliber\n" +
                "             FROM index\n" +
                "                      LEFT JOIN tmp ON index.IndicatorCode = tmp.IndicatorCode\n" +
                "             WHERE product = '大豆'\n" +
                "               AND index.IndicatorCode in (select treeID from tree where PID in(select treeID from tree where NodeName = '国内市场价格'))\n" +
                "         ) AS tmp1\n" +
                "             LEFT JOIN data ON tmp1.IndicatorCode = data.IndicatorCode";
        sparkSession.sql(ranke_tabel).createOrReplaceTempView("ranke_Table");
//            sparkSession.sql(ranke_tabel).show();*/

        String rankTableSql = "WITH parsed_content AS (" +
                "SELECT IndicatorCode, " +
                "       from_json(content, '" + jsonSchema + "') AS parsedContent " +
                "FROM index" +
                "), " +
                "tmp AS (" +
                "SELECT IndicatorCode, " +
                "       parsedContent.product.attrName AS product, " +
                "       parsedContent.BelongsArea.attrName AS BelongsArea, " +
                "       parsedContent.measure.attrName AS measure, " +
                "       parsedContent.upd_freq.attrName AS upd_freq, " +
                "       parsedContent.caliber.attrName AS caliber " +
                "FROM parsed_content " +
                "), " +
                "filtered_index AS (" +
                "SELECT index.IndicatorCode, " +
                "       index.IndicatorName, " +
                "       index.endDate, " +
                "       index.upd_freq, " +
                "       index.unified, " +
                "       tmp.product, " +
                "       tmp.BelongsArea, " +
                "       tmp.measure, " +
                "       tmp.caliber " +
                "FROM index " +
                "JOIN tmp ON index.IndicatorCode = tmp.IndicatorCode " +
                "WHERE product = '大豆' " +
                "  AND index.IndicatorCode in (select treeID from tree where PID in (select treeID from tree where NodeName = '国内市场价格')) " +
                "), " +
                "rank_Table AS (" +
                "SELECT tmp1.IndicatorCode, " +
                "       tmp1.IndicatorName, " +
                "       tmp1.endDate, " +
                "       tmp1.upd_freq, " +
                "       tmp1.unified, " +
                "       tmp1.product, " +
                "       tmp1.BelongsArea, " +
                "       tmp1.measure, " +
                "       tmp1.caliber, " +
                "       data.pubDate, " +
                "       data.measureName, " +
                "       data.measureValue, " +
                "       ROW_NUMBER() OVER (PARTITION BY tmp1.IndicatorCode ORDER BY data.pubDate DESC) AS row_num " +
                "FROM filtered_index tmp1 " +
                "JOIN data ON tmp1.IndicatorCode = data.IndicatorCode " +
                ")" +
                "SELECT * FROM rank_Table";
        sparkSession.sql(rankTableSql).createOrReplaceTempView("rank_Table");
//        sparkSession.sql(rankTableSql).show();
    }

    //  Return SQL query statement
    private static String getSql() {
        return "WITH latest_dates AS (\n" +
                "    SELECT IndicatorCode, pubDate as latest_date\n" +
                "    FROM rank_Table\n" +
                "    where row_num = 1\n" +
                "),\n" +
                "     previous_year_dates AS (\n" +
                "         SELECT t1.IndicatorCode,\n" +
                "                t1.pubDate as previous_year_date\n" +
                "         FROM rank_Table t1\n" +
                "                  INNER JOIN latest_dates t2\n" +
                "                             ON t1.IndicatorCode = t2.IndicatorCode\n" +
                "         WHERE t1.pubDate = date_add(t2.latest_date, -365)),\n" +
                "     latest_values AS (\n" +
                "         SELECT t1.IndicatorCode,\n" +
                "                t1.measureValue as latest_measure_value,\n" +
                "                t1.pubDate\n" +
                "         FROM rank_Table t1\n" +
                "                  INNER JOIN latest_dates t2\n" +
                "                             ON t1.IndicatorCode = t2.IndicatorCode AND t1.pubDate = t2.latest_date\n" +
                "     ),\n" +
                "     previous_year_values AS (\n" +
                "         SELECT t1.IndicatorCode,\n" +
                "                t1.measureValue as previous_year_measure_value,\n" +
                "                t1.pubDate\n" +
                "         FROM rank_Table t1\n" +
                "                  INNER JOIN previous_year_dates t3\n" +
                "                             ON t1.IndicatorCode = t3.IndicatorCode AND t1.pubDate = t3.previous_year_date\n" +
                "     ),\n" +
                "     yoy_tabel as (SELECT t4.IndicatorCode,\n" +
                "                          t4.pubDate,\n" +
                "                          t5.pubDate,\n" +
                "                          t4.latest_measure_value,\n" +
                "                          t5.previous_year_measure_value,\n" +
                "                          (t4.latest_measure_value - t5.previous_year_measure_value) as measure_value_difference\n" +
                "                   FROM latest_values t4\n" +
                "                            INNER JOIN previous_year_values t5\n" +
                "                                       ON t4.IndicatorCode = t5.IndicatorCode),\n" +
                "     rise_fall_table as (select product                                         as product,\n" +
                "                                IndicatorCode                                   as indicator_code,\n" +
                "                                IndicatorName                                   as indicator_name,\n" +
                "                                BelongsArea                                     as address,\n" +
                "                                measureValue                                    as price,\n" +
                "                                previous_price                                  as previous_price,\n" +
                "                                (measureValue - previous_price)                        as rise_fall,\n" +
                "                                (measureValue - previous_price) / COALESCE(NULLIF(previous_price, 0), 1) * 100 as percentage,\n" +
                "                                pubDate                                         as `to_date`,\n" +
                "                                unified                                         as unit\n" +
                "                         from (select IndicatorCode,IndicatorName,unified,BelongsArea,measure,measureValue,pubDate,product,row_num,\n" +
                "                           LEAD(measureValue) OVER (PARTITION BY IndicatorCode ORDER BY pubDate desc) AS previous_price\n" +
                "                               from rank_Table\n" +
                "                               WHERE measureValue is not null\n" +
                "                                 and row_num <= 2) tmp1\n" +
                "                         where row_num = 1)\n" +
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

