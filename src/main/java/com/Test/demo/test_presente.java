package com.Test.demo;

import com.ProduceProcess.demo.ProcessBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * DzProduce   com.ProduceProcess.demo
 * 2023-03-2023/3/31   15:49
 *
 * @author : zhangmingyue
 * @description : Process Price_table
 * @date : 2023/3/31 3:49 PM
 */
public class test_presente extends ProcessBase {
    public static void main(String[] args) throws IOException {
        String appName = "Process_PriceData_Table";
        SparkSession sparkSession = defaultSparkSession(appName);

        String filePath = "/Users/zhangmingyue/Desktop/DZ_product/src/main/resources/jlcID.txt/jlcID.txt";
        String namePath = "/Users/zhangmingyue/Desktop/DZ_product/src/main/resources/jlcID.txt/measureName.txt";
        List<String> lines = Files.readAllLines(Paths.get(filePath));
        List<String> words = Files.readAllLines(Paths.get(namePath));
        String indicatorCodes = String.join("','", lines);
        String measureNames = String.join("','", words);

        //线螺:LWG3130008504LWG  //甲醇:JC2130002151JC //大豆:DD100000002DD / 橡胶:XJ5130010125XJ // 原油:YY4130100148YY //燃料油:RLY6130100363RLY //'XM1001019207XM',
        String dataTable = String.format("(select * from st_spzs_data where IndicatorCode in (SELECT b.treeID \n" +
                "FROM st_spzs_tree a\n" +
                "INNER JOIN st_spzs_tree b ON b.pathId LIKE CONCAT('%%', a.treeid, '%%')\n" +
                "WHERE a.treeID IN (%s) AND b.category = 'dmp_item')and measureName in(%s) and pubDate <= '2023-04-28' ) t", indicatorCodes,measureNames); //pubDate between '2023-01-01' and '2023-03-30'

        String priceTable = "st_spzs_data";

        getDF(sparkSession, dataTable).createOrReplaceTempView("data");
        Dataset<Row> priceDF = sparkSession.sql(getSql());
        priceDF.show();
//        writeToTiDB(priceDF, priceTable);
        sparkSession.stop();
        }
    //  Return SQL query statement
    private static String getSql() {
        return "WITH rank_table AS (\n" +
                       "    SELECT IndicatorCode,\n" +
                       "           pubDate,\n" +
                       "           measureValue,\n" +
                       "           ROW_NUMBER() OVER (PARTITION BY IndicatorCode ORDER BY pubDate DESC) AS row_num,\n" +
                       "           LEAD(measureValue) OVER (PARTITION BY IndicatorCode ORDER BY pubDate DESC) AS yesterday_price\n" +
                       "    FROM data)\n" +  // 排序 + 获取下一行数据
                       "    SELECT IndicatorCode,\n" +
                       "           pubDate,\n" +
                       "           'percentage' AS measureName,\n" +
                       "           ROUND((measureValue - yesterday_price) / COALESCE(NULLIF(yesterday_price, 0), 1) * 100, 6) AS measureValue,\n" +
                "       CURRENT_TIMESTAMP() AS updateDate,\n" +
                "       CURRENT_TIMESTAMP() AS insertDate\n" +
                "    FROM rank_table\n" +
                       "    WHERE row_num = 1 " +  // 计算涨跌值/涨跌幅
                       "ORDER BY pubDate";
    }
}
