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
 * DZ_product   com.ProduceProcess.demo
 * 2023-04-2023/4/2   11:31
 *
 * @author : zhangmingyue
 * @description : Process Down_Consumer table
 * @date : 2023/4/2 11:31 AM
 */
public class Data2Product {
    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger logger = Logger.getLogger("SpzsIndex");
        logger.setLevel(Level.ERROR);
        //   read from configuration file, get configuration
        Properties prop = new Properties();
        InputStream inputStream = JLCAllData2Tidb.class.getClassLoader().getResourceAsStream("application.properties");
        prop.load(inputStream);

        String tidbUrl_warehouse = prop.getProperty("tidb.url_warehouse");
        String tidbUser = prop.getProperty("tidb.user");
        String tidbPassword = prop.getProperty("tidb.password");

        String tidbUrl_product = prop.getProperty("tidb.url_product");
        String tidbUser_p = prop.getProperty("tidb.user_product");
        String tidbPassword_p = prop.getProperty("tidb.password_product");

        String indexTable = "st_spzs_index";
        String dataTable = "st_spzs_data";
        String treeTable = "st_spzs_tree";

        String sinkTable_data = "st_spzs_data";
        String sinkTable_tree = "st_spzs_tree";
        String sinkTable_index = "st_spzs_index";


        SparkSession sparkSession = SparkSession.builder()
                .appName("JLCDataUnifiedFormat")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .getOrCreate();
        //      get tmpView
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, indexTable).createOrReplaceTempView("index");
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, dataTable).createOrReplaceTempView("data");
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, treeTable).createOrReplaceTempView("tree");
        //      Process Price_up_table data
        Dataset<Row> price_upDF = sparkSession.sql(getSql());
        price_upDF.show();
//        writeToTiDB(price_upDF, tidbUrl_product, tidbUser_p, tidbPassword_p, sinkTable_tree);
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

    //  Return SQL query statement
    private static String getSql(){

        return " select * from tree";
//        "select * from index";
                /*"select IndicatorCode,\n" +
                "pubDate,\n" +
                "measureName,\n" +
                "measureValue,\n" +
                "updateDate,\n" +
                "insertDate from data where IndicatorCode like 'LWG%LWG' and year(pubDate) >= '2019'";*/
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
