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
 * 2023-03-2023/3/31   15:49
 *
 * @author : zhangmingyue
 * @description : Process Price_table
 * @date : 2023/3/31 3:49 PM
 */
public class JC_Price_Table {
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

        SparkSession sparkSession = SparkSession.builder()
                .appName("JLCDataUnifiedFormat")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "8g")
                .getOrCreate();

        String indexTable = "(select * from st_spzs_index  where  IndicatorCode in " +
                "(select b.treeID from(select treeid from st_spzs_tree where treeID in " +
                "('JC2130002151JC','LWG3130008504LWG', 'DD100000002DD')) a join st_spzs_tree" +
                " b on b.pathId like concat('%',a.treeid, '%')where b.category = 'dmp_item')) t1";   //线螺:566a4557dc484579c754xl53  //甲醇:576286732d09ed469c19faa9 //大豆:100000002*/
        String dataTable = "(select * from st_spzs_data where  measureName in ('DV1','hightestPrice','price'))t";  //pubDate between '2023-01-01' and '2023-03-30'
        String priceTable = "price_data";

        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, indexTable).createOrReplaceTempView("index");
        getDF(sparkSession, tidbUrl_warehouse, tidbUser, tidbPassword, dataTable).createOrReplaceTempView("data");

        Dataset<Row> priceDF = sparkSession.sql(getSql());
        priceDF.show();
//        writeToTiDB(priceDF, tidbUrl_product, tidbUser, tidbPassword, priceTable);
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
                .load();
    }

    //  Return SQL query statement
    private static String getSql() {
        //  Get attr column
        String jsonSchema = "struct<product:struct<attrName:string>,BelongsArea:struct<attrName:string>,measure:struct<attrNameAbbr:string>>";

        return  "WITH parsed_content AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "          IndicatorName,\n" +
                "          if(unified ='元', '元/吨', unified) as unified,\n" +
                "           from_json(content, '" + jsonSchema + "') AS parsedContent\n" +
                "    FROM index " +
                "),\n tmp AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "           IndicatorName,\n" +
                "           unified,\n" +
                "           parsedContent.product.attrName AS product,\n" +
                "           parsedContent.BelongsArea.attrName AS BelongsArea,\n" +
                "           parsedContent.measure.attrNameAbbr AS measure\n" +
                "    FROM parsed_content  \n" +
                "),\n" +
                "rank_Table AS (\n" +
                "    SELECT tmp.IndicatorCode,\n" +
                "           tmp.IndicatorName,\n" +
                "           tmp.unified,\n" +
                "           tmp.product,\n" +
                "           tmp.BelongsArea,\n" +
                "           tmp.measure,\n" +
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
                "           BelongsArea,\n" +
                "           measure,\n" +
                "           pubDate,\n" +
                "           measureValue,\n" +
                "           row_num,\n" +
                "        LEAD(measureValue) OVER (PARTITION BY IndicatorCode ORDER BY pubDate desc) AS yesterday_price\n" +
                "FROM rank_Table  where row_num <= 2 ) "+
                "select IndicatorCode                                            as indicator_code,\n" +
                "       IndicatorName                                            as indicator_name,\n" +
                "       BelongsArea                                              as address,\n" +
                "       if(measure like \"%数据\", REPLACE(measure, '数据', ''),measure) as type_name,\n" +
                "       measureValue                                             as latest_price,\n" +
                "       yesterday_price                                          as yesterday_price,\n" +
                "       (measureValue - yesterday_price)                         as rise_fall,\n" +
                "       if((measureValue - yesterday_price) / COALESCE(NULLIF(yesterday_price, 0), 1) * 100 = 0,0,concat(cast(round((measureValue - yesterday_price) / COALESCE(NULLIF(yesterday_price, 0), 1) * 100, 6) as STRING), '%'))  as percentage,\n" +
                "       unified                                                  as unit,\n" +
                "       pubDate                                                  as `Date`,\n" +
                "       product                                                  as product\n" +
                "from tmp1 where row_num = 1 order by pubDate"; //and pubDate ='2023-03-30'
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
