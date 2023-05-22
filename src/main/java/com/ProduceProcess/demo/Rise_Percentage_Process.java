package com.ProduceProcess.demo;

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
public class Rise_Percentage_Process extends ProcessBase {
    public static void main(String[] args) throws IOException {
        String appName = "Process_PriceData_Table";
        SparkSession sparkSession = defaultSparkSession(appName);

        String filePath = "/Users/zhangmingyue/Desktop/DZ_product/src/main/resources/calucateID.txt";
        String namePath = "/Users/zhangmingyue/Desktop/DZ_product/src/main/resources/measureName.txt";
        List<String> lines = Files.readAllLines(Paths.get(filePath));
        List<String> words = Files.readAllLines(Paths.get(namePath));
        String indicatorCodes = String.join("','", lines);
        String measureNames = String.join("','", words);

        String dataTable = String.format("(SELECT * FROM st_spzs_data WHERE IndicatorCode IN (" +
                "    SELECT b.treeID FROM st_spzs_tree a  INNER JOIN st_spzs_tree b ON b.pathId LIKE CONCAT('%%', a.treeid, '%%')" +
                "    WHERE a.treeID IN (%S) AND b.category = 'dmp_item') AND source IN ('xhs', 'jlc') AND (" +
                "    CASE " +
                "        WHEN source = 'jlc' THEN measureName IN (%s) " +
                "        ELSE TRUE" +
                "    END)) t", indicatorCodes,measureNames);

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
                       "           measureValue,pt,\n" +
                       "           ROW_NUMBER() OVER (PARTITION BY IndicatorCode ORDER BY pubDate DESC) AS row_num,\n" +
                       "           LEAD(measureValue) OVER (PARTITION BY IndicatorCode ORDER BY pubDate DESC) AS yesterday_price\n" +
                       "    FROM data),\n" +  // 排序 + 获取下一行数据
                       "tmp AS (\n" +
                       "    SELECT IndicatorCode,\n" +
                       "           pubDate, pt,\n" +
                       "           measureValue - yesterday_price AS rise_fall,\n" +
                       "           ROUND((measureValue - yesterday_price) / COALESCE(NULLIF(yesterday_price, 0), 1) * 100, 6) AS percentage\n" +
                       "    FROM rank_table\n" +
                       "    WHERE row_num = 1)\n" +  // 计算涨跌值/涨跌幅
                       "SELECT IndicatorCode,\n" +
                       "       pubDate,\n" +
                       "       'percentage' AS measureName,\n" +
                       "       COALESCE(CAST(percentage AS STRING), 0) AS measureValue,\n" +
                       "       CURRENT_TIMESTAMP() AS updateDate,\n" +
                       "       CURRENT_TIMESTAMP() AS insertDate,\n" +
                "       'calculate' AS source,\n" +
                "        pt   " +
                       "FROM tmp\n" +
                       "UNION ALL\n" +  // 分别获取, union all 合并
                       "SELECT IndicatorCode,\n" +
                       "       pubDate,\n" +
                       "       'rise_fall' AS measureName,\n" +
                       "       COALESCE(rise_fall, 0)  AS measureValue,\n" +
                       "       CURRENT_TIMESTAMP() AS updateDate,\n" +
                       "       CURRENT_TIMESTAMP() AS insertDate,\n" +
                "       'calculate' AS source,\n" +
                "        pt   " +
                       "FROM tmp\n" +
                       "ORDER BY pubDate";
    }
}
