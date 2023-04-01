package com.JLC.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
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
import java.util.*;

/**
 * DzProduce   com.JLC.demo
 * 2023-03-2023/3/25   12:13
 *
 * @author : zhangmingyue
 * @description : JLC data 2 Tidb
 * @date : 2023/3/22 12:13 PM
 */



public class JLCDateToTIDB {
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
        String dataApiUrl = prop.getProperty("data.api.url");

        String indexTable = "jlc_index";
        String dataTable = "jlc_data";

        SparkSession sparkSession = SparkSession.builder()
                .appName("API data to Spark DataFrame")
                .master("local[*]")
                .getOrCreate();

//        Call the ApiHelper class, call API get jsondata
        ApiHelper apiHelper = new ApiHelper(dataApiUrl);
        List<String> stringList = getPathId(sparkSession, tidbUrl, tidbUser, tidbPassword, indexTable);

        for (String pathId : stringList) {
            String data_jsonBody = String.format("{" +
                    "\"idxId\": \"%s\"," +
                    "\"queryColumns\": \"idxId,valueName,value,publishDt,remark\"," +
                    "\"isPaging\": 1," +
                    "\"pageNum\": 1," +
                    "\"pageSize\": 1000" +
                    "}", pathId);
            try {
                String data_jsonResponse = apiHelper.fetchData01(data_jsonBody);
                Dataset<Row> data_dataFrame = Objects.requireNonNull(parseJsonToDataFrame_data(sparkSession, data_jsonResponse));

                //                    data_dataFrame.show();
                writeToTiDB(data_dataFrame, tidbUrl, tidbUser, tidbPassword, dataTable);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        sparkSession.stop();
    }

    // Step 1: get pathId from indextable
    private static List<String> getPathId(SparkSession sparkSession, String url, String user, String password, String table) {
        Dataset<Row> indexData = sparkSession.read()
                .format("jdbc")
                .option("url", url)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", table)
                .option("user", user)
                .option("password", password)
                .load().toDF();
        indexData.createOrReplaceTempView("index");

        String sql = "select distinct id from index ";
        Dataset<Row> rowDataset = sparkSession.sql(sql);
        return rowDataset.map(
                (MapFunction<Row, String>) row -> row.getString(0),
                Encoders.STRING()).collectAsList();
    }

    // Step 3: Convert the data to a Spark DataFrame  ****
    private static Dataset<Row> parseJsonToDataFrame_data(SparkSession spark, String jsonData) throws IOException {
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
                        new StructField("value", DataTypes.StringType, true, Metadata.empty())
                });

                List<Row> data_rows = new ArrayList<>();
                for (Map<String, String> map : content) {
                    Row row = RowFactory.create(map.get("idxId"), map.get("publishDt"), map.get("valueName"), map.get("value"));
                    data_rows.add(row);
                }

                Dataset<Row> dataDf = spark.createDataFrame(data_rows, data_schema);
                //  Query the data using Spark SQL
                dataDf.createOrReplaceTempView("data_table");
                String query = "SELECT * FROM data_table";
                return spark.sql(query);
            }
        }
        return null;
    }

    // Step 4: write to Tibd
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
