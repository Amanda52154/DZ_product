package com.ZJS.demo;

import com.JLC.demo.JLCAllData2Tidb;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * DzProduce   com.ZJS.demo
 * 2023-03-2023/3/25   12:13
 *
 * @author : zhangmingyue
 * @description : ZJS data Process
 * @date : 2023/3/21 12:13 PM
 */



public class ZJSUnifiedData {
    public static void main(String[] args) throws IOException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger logger = Logger.getLogger("SpzsIndex");
        logger.setLevel(Level.ERROR);

        //      read from configuration file, get configuration
        Properties prop = new Properties();
        InputStream inputStream = JLCAllData2Tidb.class.getClassLoader().getResourceAsStream("application.properties");
        prop.load(inputStream);

        String tidbUrl = prop.getProperty("tidb.url_warehouse");
        String tidbUser = prop.getProperty("tidb.user");
        String tidbPassword = prop.getProperty("tidb.password");

        String indicatordatavTable = "c_in_indicatordatav_copy1";/*"(select * from c_in_indicatordatav where zjs_update_time >= '2023-03-30') t";*/
        String indicatormainTable = "c_in_indicatormain";
        String systemconstTable = "c_in_systemconst";
        String dictionaryTable = "c_in_dictionary";
        String sinkTable = "st_c_in_indicatordatav";


        SparkSession sparkSession = SparkSession.builder()
                .appName("TidbDemo")
                .master("local[*]")
                .getOrCreate();


        getDF(sparkSession, tidbUrl, tidbUser, tidbPassword, indicatordatavTable).createOrReplaceTempView("indicatordatav");
        getDF(sparkSession, tidbUrl, tidbUser, tidbPassword, dictionaryTable).createOrReplaceTempView("dictionary");
        getDF(sparkSession,tidbUrl, tidbUser, tidbPassword,indicatormainTable).createOrReplaceTempView("indicatormain");
        getDF(sparkSession,tidbUrl, tidbUser, tidbPassword,systemconstTable).createOrReplaceTempView("systemconst");


       String sql = "select data.ID,\n" +
               "       data.IndicatorCode,\n" +
               "       data.InfoPublDate,\n" +
               "       data.BeginDate,\n" +
               "       if(data.EndDate = '1969-12-31 00:00:00' , '1970-01-01 08:00:01',data.EndDate) as EndDate,\n" +
               "       if(dic.coefficient is not null, data.DataValue * dic.coefficient, data.DataValue) as DataValue,\n" +
               "       data.PowerNumber,\n" +
               "       data.UpdateTime,\n" +
               "       data.JSID,\n" +
               "       data.zjs_insert_time,\n" +
               "       data.zjs_update_time\n" +
               "from indicatordatav data\n" +
               "         left join\n" +
               "     indicatormain main on data.IndicatorCode = main.IndicatorCode\n" +
               "         left join dictionary dic on main.UnitCode = dic.UnitCode";
        Dataset<Row> tidbDF = sparkSession.sql(sql);

//        tidbDF.show();
        writeToTiDB(tidbDF,tidbUrl, tidbUser, tidbPassword,sinkTable);

        sparkSession.stop();
    }

    private static Dataset<Row> getDF(SparkSession sparkSession,String url, String user, String password,String table){
        return sparkSession.read()
                .format("jdbc")
                .option("url", url)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", table)
                .option("user", user)
                .option("password", password)
                .option("connectTimeout", "10000") // 10 seconds
                .option("socketTimeout", "60000") // 60 seconds
                .load();
    }
    private static void writeToTiDB(Dataset<Row> dataFrame, String url, String user, String password,String table) {
        dataFrame.repartition(10).write()
                .mode(SaveMode.Append)
                .format("jdbc")
                .option("driver","com.mysql.jdbc.Driver")
                .option("url", url)
                .option("user", user)
                .option("password", password)
                .option("dbtable", table)
                .option("isolationLevel", "NONE")    //不开启事务
                .option("batchsize", 10000)   //设置批量插入
                .save();
    }
}
