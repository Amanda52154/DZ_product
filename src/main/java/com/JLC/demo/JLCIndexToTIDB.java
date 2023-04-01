package com.JLC.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;

import java.io.IOException;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
/**
 * DzProduce   com.JLC.demo
 * 2023-03-2023/3/25   12:13
 *
 * @author : zhangmingyue
 * @description : JLC Index 2 Tidb
 * @date : 2023/3/22 12:13 PM
 */


public class JLCIndexToTIDB {
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

        String sinkTable = "jlc_index";

        List<String> idPathList = Arrays.asList(
                "576286732d09ed469c19faa8",
                "54d2d1521e29c83934af2238",
                "576297512d09ed469c19fae6",
                "57186cba2d0956fb7da4db05",
                "54d2d1521e29c83934af2235"
        );
        SparkSession sparkSession = SparkSession.builder()
                .appName("JLCDataUnifiedFormat")
                .master("local[*]")
                .getOrCreate();
//        Call the ApiHelper class, call API get jsondata
        ApiHelper apiHelper = new ApiHelper(itemApiUrl);

        for (String idPath : idPathList) {
            String category = "dmp_item";
            try {
                String index_jsonResponse = apiHelper.fetchData(idPath, category);
//                System.out.println(index_jsonResponse);
                Dataset<Row> index_dataFrame = parseJsonToDataFrame_Index(sparkSession, index_jsonResponse);

                if (index_dataFrame != null) {
                    index_dataFrame.show();
                    writeToTiDB(index_dataFrame, tidbUrl, tidbUser, tidbPassword, sinkTable);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Step 2: Convert the data to a Spark DataFrame  ****
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

    private static String safeGetAsText(JsonNode node, String fieldName) {
        return node.has(fieldName) ? node.get(fieldName).asText() : null;
    }

    //    Step 3: write to Tibd
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
