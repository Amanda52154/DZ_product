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
import java.util.Properties;

/**
 * DzProduce   com.ProduceProcess.demo
 * 2023-03-2023/3/30   09:10
 *
 * @author : zhangmingyue
 * @description : Process price_up_down data
 * @date : 2023/3/30 9:10 AM
 */
public class PriceUpDownProcess {
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
        //      Process Price_up_table data
        Dataset<Row> price_upDF = sparkSession.sql(getSql());
        price_upDF.show();
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
        String jsonSchema = "struct<product:struct<attrName:string>,BelongsArea:struct<attrName:string>,measure:struct<attrName:string>,upd_freq:struct<attrName:string>,caliber:struct<attrName:string>>";


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
                "  AND index.IndicatorCode = '1340163828'), " +
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
    private static String getSql(){
        return "with tmp1 as (select IndicatorCode,IndicatorName,unified,BelongsArea,measure,measureValue,pubDate,product,row_num,\n" +
                "              LEAD(measureValue) OVER (PARTITION BY IndicatorCode ORDER BY pubDate desc) AS previous_price\n" +
                "      from rank_Table\n" +
                "      WHERE row_num <= 2\n" +
                ")select IndicatorCode                                   as indicator_code,\n" +
                "       IndicatorName                                   as indicator_name,\n" +
                "       measureValue                                           as price,\n" +
                "       previous_price                                  as previous_price,\n" +
                "       (measureValue - previous_price)                        as rise_fall,\n" +
                "       (measureValue - previous_price) / COALESCE(NULLIF(previous_price, 0), 1) * 100 as percentage,\n" +
                "       pubDate                                         as to_date,\n" +
                "       unified                                         as unit,\n" +
                "       product                                         as product\n" +
                "from tmp1 where row_num = 1";
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
