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
 * 2023-03-2023/3/31   16:30
 *
 * @author : zhangmingyue
 * @description : Process Price_rise_fall table
 * @date : 2023/3/31 4:30 PM
 */
public class Yoy_Process_JLC extends ProcessBase {
    public static void main(String[] args) throws IOException {

        String appName = "Process_Rise_Table";
        SparkSession sparkSession = defaultSparkSession(appName);

        String filePath = "/Users/zhangmingyue/Desktop/DZ_product/src/main/resources/jlcID.txt";
        String namePath = "/Users/zhangmingyue/Desktop/DZ_product/src/main/resources/measureName.txt";
        List<String> lines = Files.readAllLines(Paths.get(filePath));
        List<String> words = Files.readAllLines(Paths.get(namePath));
        String indicatorCodes = String.join("','", lines);
        String measureNames = String.join("','", words);

        String dataTable = String.format("(select * from st_spzs_data where IndicatorCode in (SELECT b.treeID \n" +
                "FROM st_spzs_tree a\n" +
                "INNER JOIN st_spzs_tree b ON b.pathId LIKE CONCAT('%%', a.treeid, '%%')\n" +
                "WHERE a.treeID IN (%s) AND b.category = 'dmp_item') and measureName in(%s)) t", indicatorCodes, measureNames);  //pubDate between '2023-01-01' and '2023-03-30'
        String priceRiseFallTable = "st_spzs_data";

        //get tmpView
        getDF(sparkSession, dataTable).createOrReplaceTempView("data");
        Dataset<Row> price_riseDF = sparkSession.sql(getSql());
        price_riseDF.show();
        writeToTiDB(price_riseDF, priceRiseFallTable);
        sparkSession.stop();
    }

    //  Return SQL query statement
    private static String getSql() {
        return " WITH rank_Table AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "           pubDate,\n" +
                "           measureValue,\n" +
                "           pt,\n" +
                "           ROW_NUMBER() OVER (PARTITION BY IndicatorCode ORDER BY pubDate DESC) AS row_num\n" +
                "    FROM data),\n" +         // 排序
                "latest_dates AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "           pubDate as latest_date,\n" +
                "           measureValue as latest_measure_value,\n" +
                "           pt\n" +
                "    FROM rank_table \n" +
                "    WHERE row_num = 1),\n" + // 获取最新日期数据
                "previous_year_data AS (\n" +
                "    SELECT t1.IndicatorCode,\n" +
                "           t1.pubDate,\n" +
                "           t1.measureValue,\n" +
                "           ABS(DATEDIFF(t1.pubDate, DATE_ADD(t2.latest_date, -365))) AS days_difference\n" +
                "    FROM rank_Table t1\n" +
                "    JOIN latest_dates t2 ON t1.IndicatorCode = t2.IndicatorCode\n" +
                "    WHERE YEAR(t1.pubDate) = YEAR(t2.latest_date) - 1),\n" +  // 获取去年天数差值
                "previous_year_nearest_data AS (\n" +
                "    SELECT p.IndicatorCode,\n" +
                "           p.pubDate AS previous_year_date,\n" +
                "           p.measureValue AS previous_year_measure_value\n" +
                "    FROM (\n" +
                "        SELECT IndicatorCode,\n" +
                "               pubDate,\n" +
                "               measureValue,\n" +
                "               ROW_NUMBER() OVER (PARTITION BY IndicatorCode ORDER BY days_difference) as row_num\n" +
                "        FROM previous_year_data\n" +
                "    ) p\n" +
                "    WHERE p.row_num = 1),\n" +   // 获取上年同期数据
                "yoy_table AS (\n" +
                "    SELECT t3.IndicatorCode,\n" +
                "           t3.latest_date,\n" +
                "           t4.previous_year_date,\n" +
                "           t3.latest_measure_value,\n" +
                "           t4.previous_year_measure_value,\n" +
                "           t3.pt,\n" +
                "           (t3.latest_measure_value - t4.previous_year_measure_value) AS measure_value_difference\n" +
                "    FROM latest_dates t3\n" +
                "    JOIN previous_year_nearest_data t4 ON t3.IndicatorCode = t4.IndicatorCode)\n" +  // 计算同比
                "SELECT\n" +
                "    IndicatorCode,\n" +
                "    latest_date AS pubDate,\n" +
                "    'yoy' AS measureName,\n" +
                "    COALESCE(measure_value_difference, 0) AS measureValue,\n" +
                "    current_timestamp() AS updateDate,\n" +
                "    current_timestamp() AS insertDate,\n" +
                "    'calculate' AS source,\n" +
                "    pt\n" +
                "FROM yoy_table";
    }
}

