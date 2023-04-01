package com.JLC.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.*;


/**
 * DzProduce   com.JLC.demo
 * 2023-03-2023/3/26   08:45
 *
 * @author : zhangmingyue
 * @description :
 * @date : 2023/3/26 8:45 AM
 */
public class JLCAllData2Tidb {
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
        String itemApiUrl = prop.getProperty("item.api.url");
        String dataApiUrl = prop.getProperty("data.api.url");


        List<String> idPathList = Arrays.asList(
                "576286732d09ed469c19faa8",
                "54d2d1521e29c83934af2238",
                "576297512d09ed469c19fae6",
                "57186cba2d0956fb7da4db05",
                "54d2d1521e29c83934af2235"
        );
        List<String> sinkTableList = Arrays.asList(
                "jlc_index",
                "jlc_tree",
                "jlc_data"
        );

        SparkSession sparkSession = SparkSession.builder()
                .appName("JLCDataUnifiedFormat")
//                .master("local[*]")
                .getOrCreate();
        ApiHelper apiHelper = new ApiHelper(itemApiUrl);
        ApiHelper apiHelper01 = new ApiHelper(dataApiUrl);

//      step1:  Traversing the output path, execution method
        for (String sinkTable : sinkTableList) {
            if (sinkTable.equals("jlc_index")) {

                for (String idPath : idPathList) {
                    String category = "dmp_item";
                    try {
                        String index_jsonResponse = apiHelper.fetchData(idPath, category);
                        Dataset<Row> index_dataFrame = parseJsonToDataFrame_Index(sparkSession, index_jsonResponse).repartition(10);

                        if (index_dataFrame != null) {
//                            index_dataFrame.show();
                            writeToTiDB(index_dataFrame, tidbUrl, tidbUser, tidbPassword, sinkTable);
                        } else {
                            continue;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else if (sinkTable.equals("jlc_tree")) {
                for (String idPath : idPathList) {

                    String category = "dmp_column,dmp_item";
                    try {
                        String tree_jsonResponse = apiHelper.fetchData(idPath, category);
                        Dataset<Row> tree_dataFrame = Objects.requireNonNull(parseJsonToDataFrame_tree(sparkSession, tree_jsonResponse)).repartition(10);
                        if (tree_dataFrame != null) {
//                            tree_dataFrame.show();
                            writeToTiDB(tree_dataFrame, tidbUrl, tidbUser, tidbPassword, sinkTable);
                        } else {
                            continue;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                List<String> stringList = getPathId(sparkSession, tidbUrl, tidbUser, tidbPassword);

                for (String pathId : stringList) {
                    String data_jsonBody = String.format("{" +
                            "\"idxId\": \"%s\"," +
                            "\"queryColumns\": \"idxId,valueName,value,publishDt,remark\"," +
                            "\"isPaging\": 1," +
                            "\"pageNum\": 1," +
                            "\"pageSize\": 1000" +
                            "}", pathId);
                    try {
                        String data_jsonResponse = apiHelper01.fetchData01(data_jsonBody);
                        Dataset<Row> data_dataFrame = Objects.requireNonNull(parseJsonToDataFrame_data(sparkSession, data_jsonResponse)).repartition(10);

                        if (data_dataFrame != null) {
//                            data_dataFrame.show();
                            writeToTiDB(data_dataFrame, tidbUrl, tidbUser, tidbPassword, sinkTable);
                        } else {
                            continue;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        sparkSession.stop();
    }

    //  step2: Convert the data to a Spark DataFrame
    private static Dataset<Row> parseJsonToDataFrame_Index(SparkSession sparkSession, String jsonData) throws IOException {

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode data;

        try {
            data = objectMapper.readTree(jsonData).get("data").get("content");
        } catch (JsonProcessingException e) {
            throw new RuntimeException("无法解析 JSON 数据", e);
        }
        // definition Schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("updField", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("fromDate", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("subCode", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("namePath", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("toDate", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("updFreq", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("attr", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("pId", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        // add data to  List<Row>
        List<Row> rowList = new ArrayList<>();
        for (JsonNode node : data) {
            String attrJson = null;
            try {
                attrJson = objectMapper.writeValueAsString(node.get("attr"));
            } catch (IOException e) {
                e.printStackTrace();
            }
            Row row = RowFactory.create(
                    safeGetAsText(node, "updField"),
                    safeGetAsText(node, "fromDate"),
                    safeGetAsText(node, "subCode"),
                    safeGetAsText(node, "id"),
                    safeGetAsText(node, "namePath"),
                    safeGetAsText(node, "name"),
                    safeGetAsText(node, "toDate"),
                    safeGetAsText(node, "updFreq"),
                    attrJson,
                    safeGetAsText(node, "category"),
                    safeGetAsText(node, "pId")
            );
            rowList.add(row);
        }
        return sparkSession.createDataFrame(rowList, schema);
    }

    //    Jsonnode to text
    private static String safeGetAsText(JsonNode node, String fieldName) {
        return Optional.ofNullable(node.get(fieldName)).map(JsonNode::asText).orElse(null);
    }

    private static Dataset<Row> parseJsonToDataFrame_tree(SparkSession sparkSession, String jsonData) throws IOException {

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
                StructType schema = new StructType(new StructField[]{
                        new StructField("updField", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("subCode", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("id", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("namePath", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("updFreq", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("idPath", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("category", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("pId", DataTypes.StringType, true, Metadata.empty())
                });
                // content change To List<Row>
                List<Row> rows = new ArrayList<>();
                for (Map<String, String> map : content) {
                    Row row = RowFactory.create(map.get("updField"), map.get("subCode"), map.get("id"), map.get("namePath"), map.get("name"), map.get("updFreq"), map.get("idPath"), map.get("category"), map.get("pId"));
                    rows.add(row);
                }
                Dataset<Row> dataDf = sparkSession.createDataFrame(rows, schema);
                dataDf.createOrReplaceTempView("Tree_table");
                String query = "SELECT * FROM Tree_table";
                return sparkSession.sql(query);
            }
        }
        return null;
    }

    //    Get pathid from Index
    private static List<String> getPathId(SparkSession sparkSession, String url, String user, String password) {
        String indexTable = "jlc_index";
        Dataset<Row> indexData = sparkSession.read()
                .format("jdbc")
                .option("url", url)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", indexTable)
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

    //    Step 3: write to Tibd
    private static void writeToTiDB(Dataset<Row> dataFrame, String url, String user, String password, String table) {

        dataFrame.write()
                .mode(SaveMode.Append)
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", url)
                .option("user", user)
                .option("password", password)
                .option("dbtable", table)
                .option("isolationLevel", "NONE")    //不开启事务
                .option("batchsize", 1000)   //设置批量插入
                .save();
    }
}

