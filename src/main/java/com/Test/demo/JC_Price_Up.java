package com.Test.demo;

import com.Test.demo.JLCAllData2Tidb;
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
 * DzProduce   com.ProduceProcess.demo
 * 2023-03-2023/3/31   16:30
 *
 * @author : zhangmingyue
 * @description : Process Price_up_down table
 * @date : 2023/3/31 4:30 PM
 */
public class JC_Price_Up {
    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger logger = Logger.getLogger("SpzsIndex");
        logger.setLevel(Level.ERROR);
        //   read from configuration file, get configuration
        Properties prop = new Properties();
        InputStream inputStream = JLCAllData2Tidb.class.getClassLoader().getResourceAsStream("application.properties");
        prop.load(inputStream);

        String tidbUrl_warehouse = prop.getProperty("tidb.url_warehouse");
        String tidbUrl_product = prop.getProperty("tidb.url_product");
        String tidbUser = prop.getProperty("tidb.user");
        String tidbPassword = prop.getProperty("tidb.password");

        /*List<String> DateList = Arrays.asList(
                "2023-04-03",
                "2023-03-31"
        );*/
        SparkSession sparkSession = SparkSession.builder()
                .appName("JLCDataUnifiedFormat")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .getOrCreate();

        String indexTable =  "(select * from st_spzs_index  where  IndicatorCode in ('LWG3130005585LWG','JC2130002976JC','DD1340163828DD')) t1"; //  '58257969e80c2431e8e5d3da' /线螺  '1340163828' /大豆  '57c8f3cce80c19cd2f334c88' 甲醇
        String dataTable = "(select * from st_spzs_data where measureName in ('DV1','hightestPrice','price'))t";  //and pubDate <= '2023-03-30'
        String priceUpDownTable = "price_up_down";

        //      get tmpView
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, indexTable).createOrReplaceTempView("index");
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, dataTable).createOrReplaceTempView("data");
        //      Process Price_up_table data
        Dataset<Row> price_upDF = sparkSession.sql(getSql());
        price_upDF.show();
        writeToTiDB(price_upDF, tidbUrl_product, tidbUser, tidbPassword, priceUpDownTable);
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

    //  Return SQL query statement
    private static String getSql(){
        //  Get attr column
        String jsonSchema = "struct<product:struct<attrName:string>>";
        return  "WITH parsed_content AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "          IndicatorName,\n" +
                "           unified,\n" +
                "           from_json(content, '" + jsonSchema + "') AS parsedContent\n" +
                "    FROM index " +
                "),\n" +
                "tmp AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "           IndicatorName,\n" +
                "           unified,\n" +
                "           parsedContent.product.attrName AS product\n" +
                "    FROM parsed_content \n" +
                "),\n" +
                "rank_Table AS (\n" +
                "    SELECT tmp.IndicatorCode,\n" +
                "           tmp.IndicatorName,\n" +
                "           tmp.unified,\n" +
                "           tmp.product,\n" +
                "           data.pubDate,\n" +
                "           data.measureValue,\n" +
                "           ROW_NUMBER() OVER (PARTITION BY tmp.IndicatorCode ORDER BY data.pubDate DESC) AS row_num\n" +
                "    FROM tmp\n" +
                "    JOIN data ON tmp.IndicatorCode = data.IndicatorCode" +
                "),\n" +
                "tmp1 as (SELECT IndicatorCode,\n" +
                "           IndicatorName,\n" +
                "           unified,\n" +
                "           product,\n" +
                "           pubDate,\n" +
                "           measureValue,\n" +
                "       row_num,\n" +
                "       LEAD(measureValue) OVER (PARTITION BY IndicatorCode ORDER BY pubDate desc) AS previous_price\n" +
                "FROM rank_Table  where row_num <= 2) "+
                "select IndicatorCode                                   as indicator_code,\n" +
                "       IndicatorName                                   as indicator_name,\n" +
                "       measureValue                                           as price,\n" +
                "       previous_price                                  as previous_price,\n" +
                "       (measureValue - previous_price)                        as rise_fall,\n" +
                "       (measureValue - previous_price) / COALESCE(NULLIF(previous_price, 0), 1) * 100 as percentage,\n" +
                "       pubDate                                         as to_date,\n" +
                "       unified                                         as unit,\n" +
                "       product                                         as product\n" +
                "from tmp1 where row_num = 1 ";
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
                .option("batchsize", 10000)   //设置批量插入
                .save();
    }
}
