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

        //线螺:LWG3130008504LWG  //甲醇:JC2130002151JC //大豆:DD100000002DD / 橡胶:XJ5130010125XJ // 原油:YY4130100148YY //燃料油:RLY6130100363RLY
        String dataTable = "(select * from st_spzs_data where IndicatorCode in " +
                " (select b.treeID from(select treeid from st_spzs_tree where treeID in ('DD100000002DD'))a join st_spzs_tree b on b.pathId like concat('%',a.treeid, '%')where b.category = 'dmp_item')and measureName in ('DV1','hightestPrice','price') )t";  //pubDate between '2023-01-01' and '2023-03-30' // 'DV1','hightestPrice',
        String priceTable = "st_spzs_data_1";

        getDF(sparkSession, dataTable).createOrReplaceTempView("data");

        Dataset<Row> priceDF = sparkSession.sql(getSql());
        priceDF.show();
//        writeToTiDB(priceDF, priceTable);
        sparkSession.stop();
    }
    //  Return SQL query statement
    private static String getSql() {
        //  Get attr column

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
                       "ORDER BY pubDate\n";

    }
}
