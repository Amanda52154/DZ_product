package com.Test.demo;

import com.Test.demo.JLCAllData2Tidb;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * DzProduce   com.ProduceProcess.demo
 * 2023-03-2023/3/28   11:28
 *
 * @author : zhangmingyue
 * @description : Process Price Data
 * @date : 2023/3/28 11:28 AM
 */
public class ALLPriceDataProcess {
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
        String priceTable = "price_data";
        String priceRiseFallTable = "price_rise_fall";
        String priceUpDownTable = "price_up_down";

        SparkSession sparkSession = SparkSession.builder()
                .appName("JLCDataUnifiedFormat")
                .master("local[*]")
                .getOrCreate();

//      get tmpView
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, indexTable).createOrReplaceTempView("index");
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, dataTable).createOrReplaceTempView("data");
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, treeTable).createOrReplaceTempView("tree");
        getTmpView(sparkSession);

//      Process price_table data
        String price_sql = "with tmp1 as (select IndicatorCode,IndicatorName,unified,BelongsArea,measure,pubDate,product,row_num,\n" +
                "                     MAX(measureValue) OVER (PARTITION BY IndicatorCode) AS latest_price,\n" +
                "                     MIN(measureValue) OVER (PARTITION BY IndicatorCode) AS yesterday_price\n" +
                "              from ranke_Table_0\n" +
                "              WHERE measureValue is not null\n" +
                "                and row_num <= 2\n" +
                ")\n" +
                "select IndicatorCode                                            as indicator_code,\n" +
                "       IndicatorName                                            as indicator_name,\n" +
                "       BelongsArea                                              as address,\n" +
                "       if(measure like \"%数据\", REPLACE(measure, '数据', ''),measure) as type_name,\n" +
                "       latest_price                                             as latest_price,\n" +
                "       yesterday_price                                          as yesterday_price,\n" +
                "       (latest_price - yesterday_price)                         as rise_fall,\n" +
                "       if((latest_price - yesterday_price) / COALESCE(NULLIF(yesterday_price, 0), 1) * 100 = 0,0,concat(cast(round((latest_price - yesterday_price) / COALESCE(NULLIF(yesterday_price, 0), 1) * 100, 6) as STRING), '%'))  as percentage,\n" +
                "       unified                                                  as unit,\n" +
                "       pubDate                                                  as `Date`,\n" +
                "       product                                                  as product\n" +
                "from tmp1 where row_num = 1";
        Dataset<Row> priceDF = sparkSession.sql(price_sql);
        priceDF.show();
//        writeToTiDB(priceDF, tidbUrl_product, tidbUser, tidbPassword, priceTable);

//        Process Price_rise_table data
        String price_rise_sql = "WITH latest_dates AS (\n" +
                "    SELECT IndicatorCode, pubDate as latest_date\n" +
                "    FROM ranke_table_1\n" +
                "    where row_num = 1\n" +
                "),\n" +
                "     previous_year_dates AS (\n" +
                "         SELECT t1.IndicatorCode,\n" +
                "                t1.pubDate as previous_year_date\n" +
                "         FROM ranke_table_1 t1\n" +
                "                  INNER JOIN latest_dates t2\n" +
                "                             ON t1.IndicatorCode = t2.IndicatorCode\n" +
                "         WHERE t1.pubDate = date_add(t2.latest_date, -365)),\n" +
                "     latest_values AS (\n" +
                "         SELECT t1.IndicatorCode,\n" +
                "                t1.measureValue as latest_measure_value,\n" +
                "                t1.pubDate\n" +
                "         FROM ranke_table_1 t1\n" +
                "                  INNER JOIN latest_dates t2\n" +
                "                             ON t1.IndicatorCode = t2.IndicatorCode AND t1.pubDate = t2.latest_date\n" +
                "     ),\n" +
                "     previous_year_values AS (\n" +
                "         SELECT t1.IndicatorCode,\n" +
                "                t1.measureValue as previous_year_measure_value,\n" +
                "                t1.pubDate\n" +
                "         FROM ranke_table_1 t1\n" +
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
                "                                price                                           as price,\n" +
                "                                previous_price                                  as previous_price,\n" +
                "                                (price - previous_price)                        as rise_fall,\n" +
                "                                (price - previous_price) / COALESCE(NULLIF(previous_price, 0), 1) * 100 as percentage,\n" +
                "                                pubDate                                         as `to_date`,\n" +
                "                                unified                                         as unit\n" +
                "                         from (select IndicatorCode,IndicatorName,unified,BelongsArea,measure,pubDate,product,row_num,\n" +
                "                                      MAX(measureValue) OVER (PARTITION BY IndicatorCode) AS price,\n" +
                "                                      MIN(measureValue) OVER (PARTITION BY IndicatorCode) AS previous_price\n" +
                "                               from ranke_table_1\n" +
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
//        Dataset<Row> price_riseDF = sparkSession.sql(price_rise_sql);
//        price_riseDF.show();
//        writeToTiDB(price_riseDF, tidbUrl_product, tidbUser, tidbPassword, priceRiseFallTable);

//        Process Price_up_table data
        String price_up_sql = "with tmp1 as (select IndicatorCode,IndicatorName,unified,BelongsArea,measure,pubDate,product,row_num,\n" +
                "             MAX(measureValue) OVER (PARTITION BY IndicatorCode) AS price,\n" +
                "             MIN(measureValue) OVER (PARTITION BY IndicatorCode) AS previous_price\n" +
                "      from ranke_table_1\n" +
                "      WHERE IndicatorCode = '1340163828'\n" +
                "        and row_num <= 2\n" +
                ")select IndicatorCode                                   as indicator_code,\n" +
                "       IndicatorName                                   as indicator_name,\n" +
                "       price                                           as price,\n" +
                "       previous_price                                  as previous_price,\n" +
                "       (price - previous_price)                        as rise_fall,\n" +
                "       (price - previous_price) / COALESCE(NULLIF(previous_price, 0), 1) * 100 as percentage,\n" +
                "       pubDate                                         as to_date,\n" +
                "       unified                                         as unit,\n" +
                "       product                                         as product\n" +
                "from tmp1 where row_num = 1";
//        Dataset<Row> price_upDF = sparkSession.sql(price_up_sql);
//        price_upDF.show();
//        writeToTiDB(price_upDF, tidbUrl_product, tidbUser, tidbPassword, priceUpDownTable);
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
        String getContentSql = "select distinct IndicatorCode, content from index";
        Dataset<Row> contentData = sparkSession.sql(getContentSql);

        String jsonSchema = "struct<product:struct<attrName:string>,BelongsArea:struct<attrName:string>,measure:struct<attrName:string>,upd_freq:struct<attrName:string>,caliber:struct<attrName:string>>";

        Dataset<Row> parsedData = contentData.selectExpr("IndicatorCode", "from_json(content, '" + jsonSchema + "') as parsedContent");
        parsedData.selectExpr("IndicatorCode", "parsedContent.product.attrName as product", "parsedContent.BelongsArea.attrName as BelongsArea", "parsedContent.measure.attrName as measure", "parsedContent.upd_freq.attrName as upd_freq", "parsedContent.caliber.attrName as caliber").createOrReplaceTempView("tmp");

        List<String> sqlList = Arrays.asList(
                "select treeID from tree where PID in (select treeID from tree where PID =(select treeID from tree where NodeName = '价格'))",
                "select treeID from tree where PID =(select treeID from tree where NodeName = '国内市场价格')"
        );
        for (int i = 0; i <sqlList.size() ; i++)  {
            String s = sqlList.get(i);
            String ranke_tabel = String.format("SELECT tmp1.IndicatorCode,\n" +
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
                    "               AND index.IndicatorCode in (%s)\n" +
                    "         ) AS tmp1\n" +
                    "             LEFT JOIN data ON tmp1.IndicatorCode = data.IndicatorCode", s);
            sparkSession.sql(ranke_tabel).createOrReplaceTempView("ranke_Table_" + i);
//            sparkSession.sql(ranke_tabel).show();
//            System.out.println("表名:"+"ranke_Table_" + i );
        }
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
                .option("batchsize", 1000)   //设置批量插入
                .save();
    }
}


