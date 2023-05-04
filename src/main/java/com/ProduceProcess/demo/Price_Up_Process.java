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
 * @description : Process Price_up_down table
 * @date : 2023/3/31 4:30 PM
 */
public class Price_Up_Process extends ProcessBase {
    public static void main(String[] args) throws IOException {
        String appName = "Process_UpDown_Table";
        SparkSession sparkSession = defaultSparkSession(appName);


        String indexTable = "( select *" +
                " from st_spzs_index " +
                " where" +
                " IndicatorCode in ('PG8130101331PG','DY9131019145DY','XM1001019556XM','SZ1010221476SZ','RZJB1010221602RZJB','HS3230000084HS','YMDF1010841721YMDF','ZLY1010841849ZLY','BT1012000006BT','TTJ1020000003TTJ','DLM1021000003DLM','JTM1022000003JTM','TKS4130000061TKS','XD1000000111XD','T1000000018T','L5120000030L','N6120000018N','BL810220101BL','BXG1011000305BXG')" +
                ") t1";
        //线螺:'LWG3130005585LWG' / 大豆 : 'DD1340163828DD' / 甲醇'JC2130002976JC' / 燃料油:RLY6130100646RLY  原油:YY4130100162YY  橡胶:XJ5130010138XJ
        //PG8130101331PG /MH8131019009MH(现货价) / DY9131019145DY / XM1001019556XM / DP1231019356DP(出厂价) / SZ1010221490SZ /RZJB1010221602RZJB /JD8135018124JD / HZ1340065521HZ / HS3230000084HS / YMDF1010841721YMDF / ZLY1010841849ZLY / YM1010961036YM / BT1012000006BT / XC3133000212XC / JM4100002023JM / BXG1011000305BXG/ ZJ2530002103ZJ无  QD2650003013QD 无数据/ TTJ1020000003TTJ / DLM1021000003DLM / JTM1022000003JTM / TKS4130000061TKS / XD1000000111XD / T1000000018T /L5120000030L /N6120000018N / BL810220101BL 水稻,纯碱没有价格
        String dataTable = "( select * " +
                " from st_spzs_data" +
                " where " +
                " IndicatorCode in ('PG8130101331PG','DY9131019145DY','XM1001019556XM','SZ1010221476SZ','RZJB1010221602RZJB','HS3230000084HS','YMDF1010841721YMDF','ZLY1010841849ZLY','BT1012000006BT','TTJ1020000003TTJ','DLM1021000003DLM','JTM1022000003JTM','TKS4130000061TKS','XD1000000111XD','T1000000018T','L5120000030L','N6120000018N','BL810220101BL','BXG1011000305BXG')" +
//                " and measureName in ('DV1','hightestPrice','price')" +
                ")t";  //and pubDate <= '2023-04-27' in ('DV1','hightestPrice','price')
        String priceUpDownTable = "price_up_down";

        //      get tmpView
        getDF(sparkSession, indexTable).createOrReplaceTempView("index");
        getDF(sparkSession, dataTable).createOrReplaceTempView("data");
        //      Process Price_up_table data
        Dataset<Row> price_upDF = sparkSession.sql(getSql());
        price_upDF.show();
        writeToTiDB(price_upDF, priceUpDownTable);
        sparkSession.stop();
    }

    //  Return SQL query statement
    private static String getSql() {
        //  Get attr column
        String jsonSchema = "struct<product:struct<attrName:string>>";
        return "WITH parsed_content AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "          IndicatorName,\n" +
                "           unified,\n" +
                "           from_json(content, '" + jsonSchema + "') AS parsedContent\n" +
                "    FROM index " +
                "),\n" +
                "tmp AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "           IndicatorName,\n" +
                "           unified,\n" +
                "           parsedContent.product.attrName AS product\n" +
                "    FROM parsed_content \n" +
                "),\n" +
                "rank_Table AS (\n" +
                "    SELECT tmp.IndicatorCode,\n" +
                "           tmp.IndicatorName,\n" +
                "           tmp.unified,\n" +
                "           tmp.product,\n" +
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
                "           pubDate,\n" +
                "           measureValue,\n" +
                "       row_num,\n" +
                "       LEAD(measureValue) OVER (PARTITION BY IndicatorCode ORDER BY pubDate desc) AS previous_price\n" +
                "FROM rank_Table  where row_num <= 2) " +
                "select IndicatorCode                                   as indicator_code,\n" +
                "       IndicatorName                                   as indicator_name,\n" +
                "       measureValue                                           as price,\n" +
                "       previous_price                                  as previous_price,\n" +
                "       (measureValue - previous_price)                        as rise_fall,\n" +
                "       (measureValue - previous_price) / COALESCE(NULLIF(previous_price, 0), 1) * 100 as percentage,\n" +
                "       pubDate                                         as to_date,\n" +
                "       unified                                         as unit,\n" +
                "       product                                         as product\n" +
                "from tmp1 where row_num = 1 "; //and pubDate = '2023-04-20'
    }
}
