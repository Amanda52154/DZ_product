package com.ProduceProcess.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;


/**
 * DZ_product   com.ProduceProcess.demo
 * 2023-04-2023/4/2   11:31
 *
 * @author : zhangmingyue
 * @description : Process Down_Consumer table
 * @date : 2023/4/2 11:31 AM
 */
public class Down_Consumer_Process extends ProcessBase {
    public static void main(String[] args) throws IOException {
        String appName = "Process_DownConsumer_Table";
        SparkSession sparkSession = defaultSparkSession(appName);

        String indexTable = "(select * from st_spzs_index  where  IndicatorCode in (select treeID from tree where PID ='JC2130002898JC')) t1";//620361dae4b08f1971b3fb12
        String dataTable = "(select * from st_spzs_data where pubDate > '2022-01-01')t";
        String downConsumerTable = "down_consumer";

        //      get tmpView
        getDF(sparkSession, indexTable).createOrReplaceTempView("index");
        getDF(sparkSession, dataTable).createOrReplaceTempView("data");
        //      Process Price_up_table data
        Dataset<Row> price_upDF = sparkSession.sql(getSql());
        price_upDF.show();
//        writeToTiDB(price_upDF, downConsumerTable);
        sparkSession.stop();
    }

    //  Return SQL query statement
    private static String getSql() {
        String jsonSchema = "struct<product:struct<attrName:string>,measure:struct<attrNameAbbr:string>>";

        return "WITH parsed_content AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "          IndicatorName,\n" +
                "           unified,\n" +
                "           from_json(content, '" + jsonSchema + "') AS parsedContent\n" +
                "    FROM index),\n" +
                "tmp AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "           IndicatorName,\n" +
                "           unified,\n" +
                "           parsedContent.product.attrName AS product,\n" +
                "           parsedContent.measure.attrNameAbbr AS measure\n" +
                "    FROM parsed_content \n" +
                "),\n" +
                "rank_Table AS (\n" +
                "    SELECT tmp.IndicatorCode,\n" +
                "           tmp.IndicatorName,\n" +
                "           tmp.unified,\n" +
                "           tmp.product,\n" +
                "           tmp.measure,\n" +
                "           data.pubDate,\n" +
                "           data.measureValue,\n" +
                "           ROW_NUMBER() OVER (PARTITION BY tmp.IndicatorCode ORDER BY data.pubDate DESC) AS row_num\n" +
                "    FROM tmp\n" +
                "    JOIN data ON tmp.IndicatorCode = data.IndicatorCode " +
                ")\n" +
                " select product                                         as product,\n" +
                "       IndicatorCode                                   as indicator_code,\n" +
                "       IndicatorName                                   as indicator_name,\n" +
                "       measure                                            as type_name,\n" +
                "       if(measureValue is null, 0, measureValue)        as magnitude,\n" +
                "       pubDate                                         as `date`,\n" +
                "       unified                                         as unit\n" +
                "from rank_Table where row_num = 1";
    }
}
