package com.Test.demo;

import com.JLC.demo.ApiHelper;
import com.Test.demo.JLCAllData2Tidb;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.*;
/**
 * DzProduce   com.JLC.demo
 * 2023-03-2023/3/25   12:13
 *
 * @author : zhangmingyue
 * @description : JLC Tree 2 Tidb
 * @date : 2023/3/22 12:13 PM
 */



public class JLCTreeToTIDB {
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
            String category = "dmp_column,dmp_item";
            try {
                String tree_jsonResponse = apiHelper.fetchData(idPath, category);
                System.out.println(tree_jsonResponse);
                Dataset<Row> tree_dataFrame = Objects.requireNonNull(parseJsonToDataFrame_tree(sparkSession, tree_jsonResponse));
                //                    tree_dataFrame.show();
                writeToTiDB(tree_dataFrame, tidbUrl, tidbUser, tidbPassword, sinkTable);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Step 2: Convert the data to a Spark DataFrame  ****
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


    // Step 3: write to Tibd
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


