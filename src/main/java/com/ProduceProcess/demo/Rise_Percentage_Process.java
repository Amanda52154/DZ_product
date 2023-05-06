package com.ProduceProcess.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

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
        //线螺:LWG3130008504LWG  //甲醇:JC2130002151JC //大豆:DD100000002DD / 橡胶:XJ5130010125XJ
        // 原油:YY4130100148YY //燃料油:RLY6130100363RLY
        // 'PG8130101019PG','MH8131019003MH','DY9131019121DY','XM1001019207XM','DP1231019331DP','SZ1010221463SZ','RZJB1010221563RZJB','JD8135018101JD','HZ100000002HZ','HS3230000002HS','YMDF1010841681YMDF','ZLY1010841806ZLY','YM1010961005YM','BXG1011000202BXG','BT1012000001BT','XC3133000202XC','JM4100002021JM','ZJ2530002101ZJ','QD2650003011QD','TTJ1020000001TTJ','DLM1021000001DLM','JTM1022000001JTM','TKS4130000004TKS','XD1000000001XD','T1000000001T','L5120000004L','N6120000004N','BL810000002BL'
        String dataTable = "( select *  " +
                " from st_spzs_data " +
                " where IndicatorCode in (select b.treeID from(select treeid from st_spzs_tree where treeID in ('ZJ2530002101ZJ','XM1024010142XM'))a join st_spzs_tree b on b.pathId like concat('%',a.treeid, '%')where b.category = 'dmp_item')" +
                ")t";  //pubDate between '2023-01-01' and '2023-03-30' // and measureName in ('DV1','hightestPrice','price','openingPrice')
        String priceTable = "st_spzs_data_1";

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
                       "    FROM data),\n" +  // 排序 + 获取下一行数据
                       "tmp AS (\n" +
                       "    SELECT IndicatorCode,\n" +
                       "           pubDate,\n" +
                       "           measureValue - yesterday_price AS rise_fall,\n" +
                       "           ROUND((measureValue - yesterday_price) / COALESCE(NULLIF(yesterday_price, 0), 1) * 100, 6) AS percentage\n" +
                       "    FROM rank_table\n" +
                       "    WHERE row_num = 1)\n" +  // 计算涨跌幅/涨跌值
                       "SELECT IndicatorCode,\n" +
                       "       pubDate,\n" +
                       "       'percentage' AS measureName,\n" +
                       "       CAST(percentage AS STRING) AS measureValue,\n" +
                       "       CURRENT_TIMESTAMP() AS updateDate,\n" +
                       "       CURRENT_TIMESTAMP() AS insertDate\n" +
                       "FROM tmp\n" +
                       "UNION ALL\n" +  // 分别获取, union all 合并
                       "SELECT IndicatorCode,\n" +
                       "       pubDate,\n" +
                       "       'rise_fall,ring_ratio' AS measureName,\n" +
                       "       rise_fall AS measureValue,\n" +
                       "       CURRENT_TIMESTAMP() AS updateDate,\n" +
                       "       CURRENT_TIMESTAMP() AS insertDate\n" +
                       "FROM tmp\n" +
                       "ORDER BY pubDate";
    }
}
