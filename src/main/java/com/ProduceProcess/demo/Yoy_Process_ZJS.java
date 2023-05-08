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
public class Yoy_Process_ZJS extends ProcessBase {
    public static void main(String[] args) throws IOException {

        String appName = "Process_Rise_Table";
        SparkSession sparkSession = defaultSparkSession(appName);

        String filePath = "/Users/zhangmingyue/Desktop/DZ_product/src/main/java/com/ProduceProcess/demo/0.txt";
        List<String> lines = Files.readAllLines(Paths.get(filePath));
        String indicatorCodes = String.join("','", lines);

        String dataTable = String.format("(select * from st_spzs_data where IndicatorCode in (SELECT b.treeID \n" +
                "FROM st_spzs_tree a\n" +
                "INNER JOIN st_spzs_tree b ON b.pathId LIKE CONCAT('%%', a.treeid, '%%')\n" +
                "WHERE a.treeID IN (%s) AND b.category = 'dmp_item') and pubDate <= '2023-04-28') t", indicatorCodes);  //pubDate between '2023-01-01' and '2023-03-30' //
        String priceRiseFallTable = "st_spzs_data_1";

        //get tmpView
        getDF(sparkSession, dataTable).createOrReplaceTempView("data");
        Dataset<Row> price_riseDF = sparkSession.sql(getSql());
        price_riseDF.show();
        writeToTiDB(price_riseDF, priceRiseFallTable);
        sparkSession.stop();
    }

    //  Return SQL query statement
    private static String getSql() {
        return "WITH rank_Table AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "           pubDate,\n" +
                "           measureValue,\n" +
                "           ROW_NUMBER() OVER (PARTITION BY IndicatorCode ORDER BY pubDate DESC) AS row_num " +
                "FROM data),\n" +  // 排序
                " latest_dates AS ( SELECT IndicatorCode,  pubDate as latest_date, measureValue as latest_measure_value " +
                "    FROM rank_table where row_num = 1),\n" + // 获取最新日期数据
                "previous_year_data AS (\n" +
                "    SELECT t1.IndicatorCode,\n" +
                "           t1.pubDate,\n" +
                "           t1.measureValue,\n" +
                "           ABS(DATEDIFF(t1.pubDate, DATE_ADD(t2.latest_date, -365))) AS days_difference\n" +
                "    FROM rank_Table t1\n" +
                "    JOIN latest_dates t2 ON t1.IndicatorCode = t2.IndicatorCode\n" +
                "    WHERE YEAR(t1.pubDate) = YEAR(t2.latest_date) - 1\n" +
                "),\n" +  // 获取去年天数差值
                "min_days_difference AS (\n" +
                "    SELECT IndicatorCode, MIN(days_difference) AS min_difference\n" +
                "    FROM previous_year_data\n" +
                "    GROUP BY IndicatorCode\n" +
                "),\n" +  // 获取最小天数
                "previous_year_nearest_data AS (\n" +
                "    SELECT DISTINCT p.IndicatorCode,\n" +
                "           p.pubDate AS previous_year_date,\n" +
                "           p.measureValue AS previous_year_measure_value\n" +
                "    FROM previous_year_data p\n" +
                "    JOIN min_days_difference m ON p.IndicatorCode = m.IndicatorCode AND p.days_difference = m.min_difference \n" +
                "),\n" + // 获取上年同期数据
                "yoy_table AS (\n" +
                "    SELECT t3.IndicatorCode,\n" +
                "           t3.latest_date,\n" +
                "           t4.previous_year_date,\n" +
                "           t3.latest_measure_value,\n" +
                "           t4.previous_year_measure_value,\n" +
                "           (t3.latest_measure_value - t4.previous_year_measure_value) AS measure_value_difference\n" +
                "    FROM latest_dates t3\n" +
                "    JOIN previous_year_nearest_data t4 ON t3.IndicatorCode = t4.IndicatorCode\n" +  // 计算同比
                ")\n" +
                "SELECT\n" +
                "    IndicatorCode,\n" +
                "    latest_date AS pubDate,\n" +
                "    'yoy' AS measureName,\n" +
                "    COALESCE(measure_value_difference, 0) AS measureValue,\n" +
                "    current_timestamp() AS updateDate,\n" +
                "    current_timestamp() AS insertDate\n" +
                "FROM yoy_table";
    }
}

