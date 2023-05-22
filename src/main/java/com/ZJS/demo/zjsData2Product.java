package com.ZJS.demo;

import com.JLC.demo.ApiHelper;
import com.Test.demo.JLCAllData2Tidb;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

/**
 * DZ_product   com.ProduceProcess.demo
 * 2023-04-2023/4/2   11:31
 *
 * @author : zhangmingyue
 * @description : Process Down_Consumer table
 * @date : 2023/4/2 11:31 AM
 */
public class zjsData2Product extends ApiHelper {
    public zjsData2Product(String apiUrl) {
        super(apiUrl);
    }
    public static void main(String[] args) throws IOException {
        //   read from configuration file, get configuration
        Properties prop = new Properties();
        InputStream inputStream = JLCAllData2Tidb.class.getClassLoader().getResourceAsStream("application.properties");
        prop.load(inputStream);

        String tidbUrl_jy = prop.getProperty("tidb.url_jy");
        String tidbUser_jy = prop.getProperty("tidb.user_jy");
        String tidbPassword_jy = prop.getProperty("tidb.password_jy");

        // Get command line arguments
        String mode = args[0];
        String startDate = args[1];
        String endDate = args[2];

        // Determine partition condition
        String partitionCondition;
        if ("FULL".equalsIgnoreCase(mode)) {
            partitionCondition = String.format("AND zjs_update_time < '%s' ", startDate);
        } else if ("INC".equalsIgnoreCase(mode)) {
            partitionCondition = String.format("AND zjs_update_time BETWEEN '%s' AND '%s'", startDate, endDate);
        } else {
            System.err.println("Invalid mode: " + mode);
            System.exit(-1);
            return;
        }


        String appName = "Data2Product";
        SparkSession sparkSession = defaultSparkSession(appName);
        String filePath = "/Users/zhangmingyue/Desktop/DZ_product/src/main/resources/all_zjsID2Product.txt";

        List<String> lines = Files.readAllLines(Paths.get(filePath));
        String indicatorCodes = String.join("','", lines);
        String dataTable = String.format("(select *, %s as pt from c_in_indicatordatav where IndicatorCode in (%s) %s) t", startDate, indicatorCodes, partitionCondition);
        String datavTable1 = "c_in_indicatordatav";

        Dataset<Row> price_upDF = getDF(sparkSession, tidbUrl_jy, tidbUser_jy, tidbPassword_jy, dataTable);
        price_upDF.show();
        writeToTiDB(price_upDF, datavTable1);

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
}