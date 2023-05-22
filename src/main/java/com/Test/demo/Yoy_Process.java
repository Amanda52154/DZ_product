package com.Test.demo;

import com.ProduceProcess.demo.ProcessBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * DzProduce   com.ProduceProcess.demo
 * 2023-03-2023/3/31   16:30
 *
 * @author : zhangmingyue
 * @description : Process Price_rise_fall table
 * @date : 2023/3/31 4:30 PM
 */
public class Yoy_Process extends ProcessBase {
    public static void main(String[] args) throws IOException {

        String appName = "Process_Rise_Table";
        SparkSession sparkSession = defaultSparkSession(appName);

        //线螺:LWG3130008504LWG  //甲醇:JC2130002151JC //大豆:DD100000002DD / 橡胶:XJ5130010125XJ // 原油:YY4130100148YY //燃料油:RLY6130100363RLY
        String dataTable = "( select * " +
                " from st_spzs_data " +
                " where IndicatorCode in (select b.treeID from(select treeid from st_spzs_tree where treeID ='YY4130100148YY')a join st_spzs_tree b on b.pathId like concat('%',a.treeid, '%')where b.category = 'dmp_item') and measureName = 'price'" +
                " )t";  //pubDate between '2023-01-01' and '2023-03-30' // and measureName in ('DV1','hightestPrice','price','openingPrice')
        String priceRiseFallTable = "st_spzs_data";

        //get tmpView
        getDF(sparkSession, dataTable).createOrReplaceTempView("data");
        Dataset<Row> price_riseDF = sparkSession.sql(getSql());
//        price_riseDF.show();
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
                "    current_timestamp() AS insertDate,\n" +"    'st_spzs_data_1' AS source\n" +
                "FROM yoy_table";
    }
}

