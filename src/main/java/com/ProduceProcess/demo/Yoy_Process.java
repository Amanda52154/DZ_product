package com.ProduceProcess.demo;

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
                " where IndicatorCode in (select b.treeID from(select treeid from st_spzs_tree where treeID in ('PG8130101019PG','DY9131019121DY','XM1001019207XM','SZ1010221463SZ','RZJB1010221563RZJB','HS3230000002HS','YMDF1010841681YMDF','ZLY1010841806ZLY','BXG1011000202BXG','BT1012000001BT','TTJ1020000001TTJ','DLM1021000001DLM','JTM1022000001JTM','TKS4130000004TKS','XD1000000001XD','T1000000001T','L5120000004L','N6120000004N','BL810000002BL'))a join st_spzs_tree b on b.pathId like concat('%',a.treeid, '%')where b.category = 'dmp_item')" +
                " )t";  //pubDate between '2023-01-01' and '2023-03-30' // and measureName in ('DV1','hightestPrice','price','openingPrice')
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
                "    SELECT t1.IndicatorCode,\n" +
                "           t1.pubDate as previous_year_date,\n" +
                "           t1.measureValue as previous_year_measure_value\n" +
                "    FROM rank_table t1 JOIN latest_dates t2 ON t1.IndicatorCode = t2.IndicatorCode\n" +
                "    WHERE t1.pubDate = date_add(t2.latest_date, -365)),\n" + // 获取上年同期数据
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

