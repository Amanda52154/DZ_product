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
public class Yoy_Process_test extends ProcessBase {
    public static void main(String[] args) throws IOException {

        String appName = "Process_Rise_Table";
        SparkSession sparkSession = defaultSparkSession(appName);

        String filePath = "/Users/zhangmingyue/Desktop/DZ_product/src/main/java/com/ProduceProcess/demo/priceID.txt";
        List<String> lines = Files.readAllLines(Paths.get(filePath));
        String indicatorCodes = String.join("','", lines);

        String dataTable = String.format("(select * from st_spzs_data where IndicatorCode in (select b.treeID from(select treeid from st_spzs_tree where treeID in (%s))a join st_spzs_tree b on b.pathId like concat('%%',a.treeid, '%%') where b.category = 'dmp_item') and pubDate <= '2023-04-28') t", indicatorCodes);  //pubDate between '2023-01-01' and '2023-03-30' // and measureName in ('DV1','hightestPrice','price','openingPrice')
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
                "previous_year_dates AS (\n" +
                "    SELECT t2.IndicatorCode,\n" +
                "           t2.latest_date as previous_year_date,\n" +
                "            COALESCE(t1.measureValue, t2.latest_measure_value) as previous_year_measure_value\n" +
                "    FROM rank_table t1 right JOIN latest_dates t2 ON t1.IndicatorCode = t2.IndicatorCode\n" +
                "    and t1.pubDate = date_add(t2.latest_date, -365)),\n" + // 获取上年同期数据
                "yoy_tabel as (SELECT t3.IndicatorCode,\n" +
                "                     t3.latest_date,\n" +
                "                     t4.previous_year_date,\n" +
                "                     t3.latest_measure_value,\n" +
                "                     t4.previous_year_measure_value,\n" +
                "                     (t3.latest_measure_value - t4.previous_year_measure_value) as measure_value_difference\n" +
                "              FROM latest_dates t3 JOIN previous_year_dates t4\n" +
                "                                  ON t3.IndicatorCode = t4.IndicatorCode)\n" +  // 计算同比
                "select\n" +
                "IndicatorCode,\n" +
                "latest_date         as pubDate ,\n" +
                "'yoy'               as measureName,\n" +
                "COALESCE(measure_value_difference, 0) as measureValue,\n" +
                "current_timestamp()     as updateDate,\n" +
                "current_timestamp()     as insertDate\n" +
                "from yoy_tabel";
    }
}

