package com.JLC.demo;

import org.apache.spark.sql.SparkSession;
import java.io.IOException;


/**
 * DzProduce   com.JLC.demo
 * 2023-03-2023/3/30   14:28
 *
 * @author : zhangmingyue
 * @description : Process data unified
 * @date : 2023/3/30 2:28 PM
 */
public class DataUnifiedFormat extends ApiHelper {
    public DataUnifiedFormat(String apiUrl) {
        super(apiUrl);
    }

    public static void main(String[] args) throws IOException {
        String appName = "DataUnifiedFormat";
        SparkSession sparkSession = defaultSparkSession(appName);

        String dataTable = "jlc_data"; /*"(select * from jlc_data where publishDt > '2022-10-01') t";*/
        String sinkTable = "st_jlc_data";

        getDF(sparkSession, dataTable).createOrReplaceTempView("data");

//        sparkSession.sql(getSql()).show(false);
        writeToTiDB(sparkSession.sql(getSql()), sinkTable);
        sparkSession.stop();
    }
    private static String getSql(){
        return "select\n" +
                "idxId as IndicatorCode,\n" +
                "publishDt as pubDate,\n" +
                "valueName as measureName,\n" +
                "value as measureValue,\n" +
                "current_timestamp() as updateDate,\n" +
                "current_timestamp() as insertDate,\n" +
                "pt\n" +
                "from data ";
    }
}
