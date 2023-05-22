package com.JLC.demo;

import com.Test.demo.JLCAllData2Tidb;
import org.apache.spark.sql.*;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * DzProduce   com.JLC.demo
 * 2023-03-2023/3/25   12:13
 *
 * @author : zhangmingyue
 * @description : JLC data 2 Tidb
 * @date : 2023/3/22 12:13 PM
 */



public class JLCDateToTIDB extends ApiHelper{
    public JLCDateToTIDB(String apiUrl) {
        super(apiUrl);
    }

    public static void main(String[] args) throws IOException {

        String appName = "JLCDateToTIDB";
        SparkSession sparkSession = defaultSparkSession(appName);

        // read from configuration file, get configuration
        Properties prop = new Properties();
        InputStream inputStream = JLCAllData2Tidb.class.getClassLoader().getResourceAsStream("application.properties");
        prop.load(inputStream);

        // Get command line arguments
        String mode = args[0];
        String startDate = args[1];
        String endDate = args[2];

        // Determine partition condition
        String partitionCondition;
        if ("FULL".equalsIgnoreCase(mode)) {
            partitionCondition = String.format("where publishDt < '%s' ", startDate);
        } else if ("INC".equalsIgnoreCase(mode)) {
            partitionCondition = String.format("where publishDt BETWEEN '%s' AND '%s'", startDate, endDate);
        } else {
            System.err.println("Invalid mode: " + mode);
            System.exit(-1);
            return;
        }

        String dataApiUrl = prop.getProperty("data.api.url");
        String dataTable = "jlc_data";

        String filePath = "/Users/zhangmingyue/Desktop/DZ_product/src/main/resources/all_jlcID2Product.txt";

       /* List<String> stringList = Files.readAllLines(Paths.get(filePath));
        for (String pathId : stringList) {
            String data_jsonBody = String.format("{" +
                    "\"idxId\": \"%s\"," +
                    "\"queryColumns\": \"idxId,valueName,value,publishDt,remark\"," +
                    "\"isPaging\": 1," +
                    "\"pageNum\": 1," +
                    "\"pageSize\": 1000" +
                    "}", pathId);
            try {
                String data_jsonResponse = sendPostRequest(dataApiUrl,data_jsonBody);
                Dataset<Row> data_dataFrame = Objects.requireNonNull(parseJsonToDataFrame_data(sparkSession, data_jsonResponse,startDate ,partitionCondition));
                data_dataFrame.show();
//                writeToTiDB(data_dataFrame, dataTable);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/
        String pathId = Files.readAllLines(Paths.get(filePath)).get(0);
        String data_jsonBody = String.format("{" +
                "\"idxId\": \"%s\"," +
                "\"queryColumns\": \"idxId,valueName,value,publishDt,remark\"," +
                "\"isPaging\": 1," +
                "\"pageNum\": 1," +
                "\"pageSize\": 1000" +
                "}", pathId);
        String data_jsonResponse = sendPostRequest(dataApiUrl,data_jsonBody);
        Dataset<Row> data_dataFrame = Objects.requireNonNull(parseJsonToDataFrame_data(sparkSession, data_jsonResponse,startDate ,partitionCondition));
        data_dataFrame.show();
//        writeToTiDB(data_dataFrame, dataTable);

        sparkSession.stop();
    }


    // Step 3: Convert the data to a Spark DataFrame  ****
    private static Dataset<Row> parseJsonToDataFrame_data(SparkSession spark, String jsonData, String startDate,String partitionCondition) throws IOException {
        JsonParser parser = new JsonParser();
        JsonObject responseObject = parser.parse(jsonData).getAsJsonObject();

        if (responseObject.has("data") && responseObject.get("data").isJsonObject()) {
            JsonObject dataObject = responseObject.getAsJsonObject("data");

            if (dataObject.has("content") && dataObject.get("content").isJsonArray()) {
                JsonArray contentArray = dataObject.getAsJsonArray("content");

                Gson gson = new Gson();
                Type listType = new TypeToken<List<Map<String, Object>>>() {
                }.getType();
                List<Map<String, String>> content = gson.fromJson(contentArray, listType);

                StructType data_schema = new StructType(new StructField[]{
                        new StructField("idxId", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("publishDt", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("valueName", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("value", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("remark", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("dataId", DataTypes.StringType, true, Metadata.empty())
                });

                List<Row> data_rows = new ArrayList<>();
                for (Map<String, String> map : content) {
                    Row row = RowFactory.create(map.get("idxId"), map.get("publishDt"), map.get("valueName"), map.get("value"), map.get("remark"), map.get("dataId"));
                    data_rows.add(row);
                }

                Dataset<Row> dataDf = spark.createDataFrame(data_rows, data_schema);
                //  Query the data using Spark SQL
                dataDf.createOrReplaceTempView("data_table");
                String query = String.format("SELECT idxId,publishDt,valueName,value, '%s' as pt FROM data_table %s", startDate,partitionCondition);
                return spark.sql(query);
            }
        }
        return null;
    }

}
