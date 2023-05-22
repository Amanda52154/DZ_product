package com.ProduceProcess.demo;

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
 * DZ_product   com.ProduceProcess.demo
 * 2023-04-2023/4/15   10:33
 *
 * @author : zhangmingyue
 * @description :
 * @date : 2023/4/15 10:33 AM
 */
public class ProcessBase {
    public  static SparkSession defaultSparkSession(String appName) throws IOException {
        /*
    创建SparkSession对象
    :return: SparkSession对象
    */
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger logger = Logger.getLogger("SpzsIndex");
        logger.setLevel(Level.ERROR);

        return SparkSession.builder()
                .appName(appName)
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "8g")
                .getOrCreate();
    }

    private static String[] getUrl() throws IOException {
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

        return new String[]{tidbUrl_warehouse, tidbUser, tidbPassword, tidbUrl_product, tidbUser_p, tidbPassword_p};
    }

    //      Get tableView function
    protected static Dataset<Row> getDF(SparkSession sparkSession, String table) throws IOException {
        String[] result = getUrl();
        return sparkSession.read()
                .format("jdbc")
                .option("url", result[0])
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", table)
                .option("user", result[1])
                .option("password", result[2])
                .load();
    }

    //  write to Tidb
    protected static void writeToTiDB(Dataset<Row> dataFrame, String table) throws IOException {
        String[] result = getUrl();
        dataFrame.repartition(10).write()
                .mode(SaveMode.Append)
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", result[3])
                .option("user", result[4])
                .option("password", result[5])
                .option("dbtable", table)
                .option("isolationLevel", "NONE")    //不开启事务
                .option("batchsize", 10000)   //设置批量插入
                .save();
    }

}
