package com.JLC.demo;

import okhttp3.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * DzProduce   com.JLC.demo
 * 2023-03-2023/3/27   11:03
 *
 * @author : zhangmingyue
 * @description : API请求、JSON解析
 * @date : 2023/3/27 11:03 AM
 */
public class ApiHelper {
    private final String apiUrl;

    public ApiHelper(String apiUrl) {
        this.apiUrl = apiUrl;
}
    public  static SparkSession defaultSparkSession(String appName) throws IOException {
        /*
    创建SparkSession对象
    :return: SparkSession对象
    */
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger logger = Logger.getLogger("SpzsIndex");
        logger.setLevel(Level.ERROR);

        return SparkSession.builder()
                .appName(appName)
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "8g")
                .getOrCreate();
    }

    private static String[] getUrl() throws IOException {
        //   read from configuration file, get configuration
        Properties prop = new Properties();
        InputStream inputStream = JLCAllData2Tidb.class.getClassLoader().getResourceAsStream("application.properties");
        prop.load(inputStream);
        String tidbUrl_warehouse = prop.getProperty("tidb.url_warehouse");
        String tidbUser = prop.getProperty("tidb.user");
        String tidbPassword = prop.getProperty("tidb.password");

        String tidbUrl_product = prop.getProperty("tidb.url_product");
        String tidbUser_p = prop.getProperty("tidb.user_product");
        String tidbPassword_p = prop.getProperty("tidb.password_product");

        return new String[]{tidbUrl_warehouse, tidbUser, tidbPassword, tidbUrl_product, tidbUser_p, tidbPassword_p};
    }


    public String fetchData(String idPath, String categorys) throws IOException {
        String jsonBody = createJsonBody(idPath, categorys);
        return  sendPostRequest(apiUrl, jsonBody);
    }
    public String fetchData01(String jsonBody) throws IOException {
        return  sendPostRequest(apiUrl, jsonBody);
    }

    protected static Dataset<Row> getDF(SparkSession sparkSession, String table) throws IOException {
        String[] result = getUrl();
        return sparkSession.read()
                .format("jdbc")
                .option("url", result[0])
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", table)
                .option("user", result[1])
                .option("password", result[2])
                .load();
    }

    //  write to Tidb
    protected static void writeToTiDB(Dataset<Row> dataFrame, String table) throws IOException {
        String[] result = getUrl();
        dataFrame.repartition(10).write()
                .mode(SaveMode.Append)
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", result[0])
                .option("user", result[1])
                .option("password", result[2])
                .option("dbtable", table)
                .option("isolationLevel", "NONE")    //不开启事务
                .option("rewriteBatchedStatements", "true")
                .option("batchsize", 5000)   //设置批量插入
                .save();
    }

//    Get jsonbody
    private String createJsonBody(String idPath, String categorys) {
        return String.format("{" +
                "\"categorys\": \"%s\"," +
                "\"idPath\": \"%s\"," +
                "\"isPaging\": 1," +
                "\"pageNum\": 1," +
                "\"pageSize\": 1000," +
                "\"subCode\": \"SP,SO,SI,SA,SR,SC,JC,CB,ST,ZJ\"," +
                "\"queryColumns\": \"id,subCode,name,pId,subCode,updFreq,updField,fromDate,toDate,attr,category,namePath,idPath\"" +
                "}", categorys, idPath);
    }
// Request API get data
    private String sendPostRequest(String url,String jsonBody) throws IOException {
        OkHttpClient client = new OkHttpClient();
        RequestBody requestBody = RequestBody.create(jsonBody, MediaType.parse("application/json; charset=utf-8"));
        Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .addHeader("Content-Type", "application/json")
                .build();
        Response response = client.newCall(request).execute();

        if (response.body() != null) {
            return response.body().string();
        }
        return null;
    }
}