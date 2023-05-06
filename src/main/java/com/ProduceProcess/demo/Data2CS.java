package com.ProduceProcess.demo;

import com.JLC.demo.JLCAllData2Tidb;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

/**
 * DZ_product   com.ProduceProcess.demo
 * 2023-04-2023/4/2   11:31
 *
 * @author : zhangmingyue
 * @description : Process Down_Consumer table
 * @date : 2023/4/2 11:31 AM
 */
public class Data2CS extends ProcessBase {
    public static void main(String[] args) throws IOException {

        //   read from configuration file, get configuration
        Properties prop = new Properties();
        InputStream inputStream = JLCAllData2Tidb.class.getClassLoader().getResourceAsStream("application.properties");
        prop.load(inputStream);

        String tidbUrl_warehouse = prop.getProperty("tidb.url_warehouse");
        String tidbUser = prop.getProperty("tidb.user");
        String tidbPassword = prop.getProperty("tidb.password");

        String tidbUrl_jy = prop.getProperty("tidb.url_jy");
        String tidbUser_jy = prop.getProperty("tidb.user_jy");
        String tidbPassword_jy = prop.getProperty("tidb.password_jy");

        /*String tidbUrl_product = prop.getProperty("tidb.url_product");
        String tidbUser_p = prop.getProperty("tidb.user_product");
        String tidbPassword_p = prop.getProperty("tidb.password_product");*/

        String appName = "Data2CS";
        SparkSession sparkSession = defaultSparkSession(appName);

        /*String resultTable = "( select id, " +
                "category_id," +
                "grade_id," +
                "region_id," +
                "position_id," +
                "task_time," +
                "measurefield," +
                "measureUnit," +
                "measureValue\n" +
                "from spzs_atom) t";
        String adjustmentTable = "(SELECT " +
                                 " case " +
                                 " when unified_number = '0' then concat('dazao',cast(unified_number as CHAR), exponent_id, 'root') " +
                                 " else cast(unified_number as CHAR) " +
                                 " end as IndicatorCode," +
                                 " category_id," +
                                 " grade_id," +
                                 " region_id," +
                                 " position_id" +
                                 " FROM compile_unified_adjustment ) t ";*/
        String filePath = "/Users/zhangmingyue/Desktop/DZ_product/src/main/java/com/ProduceProcess/demo/longString.txt";

        List<String> lines = Files.readAllLines(Paths.get(filePath));
        String indicatorCodes = String.join("','", lines);
        String dataTable = String.format("(select * from c_in_indicatordimension where IndicatorCode in (%s)) t", indicatorCodes);

        String datavTable1 = "c_in_indicatordimension_copy1";

//        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, indexTable).createOrReplaceTempView("index");
//        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, dataTable).createOrReplaceTempView("data");
//        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, treeTable).createOrReplaceTempView("tree");
//        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, priceTable).createOrReplaceTempView("price");
//        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, riseTable).createOrReplaceTempView("rise");
//        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, upTable).createOrReplaceTempView("up");
//        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, downTable).createOrReplaceTempView("down");
//        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, resultTable).createOrReplaceTempView("result");
//        getDF(sparkSession, tidbUrl_product, tidbUser, tidbPassword, adjustmentTable).createOrReplaceTempView("adjustment");

        //  Process Price_up_table data
//        Dataset<Row> price_upDF = sparkSession.sql(getSql());
        Dataset<Row> price_upDF = getDF(sparkSession, tidbUrl_jy, tidbUser_jy, tidbPassword_jy, dataTable);
        price_upDF.show();
        writeToTiDB(price_upDF, tidbUrl_warehouse, tidbUser, tidbPassword, datavTable1);
        sparkSession.stop();
    }

    //      Get tableView function
    private static Dataset<Row> getDF(@NotNull SparkSession sparkSession, String url, String user, String password, String table) {
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
    private static String getSql() {

        return "select  \n" +
                " adj.IndicatorCode              as   IndicatorCode,\n" +
                " DATE_TRUNC('day', result.task_time)     as   pubDate,\n" +
                " result.measurefield               as   measureName,\n" +
                " result.measureValue               as   measureValue from result  join  adjustment  adj on  adj.category_id = result.category_id and adj.grade_id = result.grade_id and adj.region_id = result.region_id and adj.position_id = result.position_id order by measureValue desc";
 /* "select adj.id                  as   id," +
                " adj.unified_number             as   IndicatorCode," +
                "adj.name                        as   IndicatorName," +
                "                                as   EndDate," +
                "          as   measureFiled ," +
                "info.detail_json                as   content," +
                "                  as   source," +
                "info.task_type                  as   upd_freq," +
                "info.update_time                as   unified  ," +
                "                                as   IndicatorCode_tmp  from info  join adjustment  adj on  info.id = adj.exponent_id ";*/

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
                .option("batchsize", 5000)   //设置批量插入
                .save();
    }
}
