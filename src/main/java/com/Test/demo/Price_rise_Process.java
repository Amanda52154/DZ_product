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
public class Price_rise_Process extends ProcessBase {
    public static void main(String[] args) throws IOException {

        String appName = "Process_Rise_Table";
        SparkSession sparkSession = defaultSparkSession(appName);

        String indexTable = "(select * from st_spzs_index  where  IndicatorCode in (select b.treeID from(select treeid from st_spzs_tree where treeID in ('XJ5130010126XJ','YY4130100160YY','RLY6130100900RLY')) a join st_spzs_tree b on b.pathId like concat('%',a.treeid, '%')where b.category = 'dmp_item')) t1";
        //线螺:LWG3130008562LWG //甲醇:JC2130002975JC //大豆:DD100000003DD // 橡胶:XJ5130010126XJ // 原油:YY4130100160YY // 燃料油:RLY6130100900RLY
        String dataTable = "(select * from st_spzs_data where  IndicatorCode in (select b.treeID from(select treeid from st_spzs_tree where treeID in ('XJ5130010126XJ','YY4130100160YY','RLY6130100900RLY')) a join st_spzs_tree b on b.pathId like concat('%',a.treeid, '%')where b.category = 'dmp_item') and measureName = 'price' )t"; // and pubDate between '2022-01-01' and '2023-03-30' //'DV1','hightestPrice',
        String priceRiseFallTable = "price_rise_fall";

        //get tmpView
        getDF(sparkSession, indexTable).createOrReplaceTempView("index");
        getDF(sparkSession, dataTable).createOrReplaceTempView("data");
        getTmpView(sparkSession);

        Dataset<Row> price_riseDF = sparkSession.sql(getSql());
        price_riseDF.show();
        writeToTiDB(price_riseDF, priceRiseFallTable);
        sparkSession.stop();
    }

    //  Get tmpView function
    private static void getTmpView(SparkSession sparkSession) {
        //  Get attr column
        String jsonSchema = "struct<product:struct<attrName:string>,BelongsArea:struct<attrName:string>>";

        String rankTableSql = "WITH parsed_content AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "          IndicatorName,\n" +
                "           '元/吨' as unified,\n" +
                "           from_json(content, '" + jsonSchema + "') AS parsedContent\n" +
                "    FROM index " +
                "),\n" +
                "tmp AS (\n" +
                "    SELECT IndicatorCode,\n" +
                "           IndicatorName,\n" +
                "           unified,\n" +
                "           parsedContent.product.attrName AS product,\n" +
                "           parsedContent.BelongsArea.attrName AS BelongsArea\n" +
                "    FROM parsed_content \n" +
                "),\n" +
                "rank_Table AS (\n" +
                "    SELECT tmp.IndicatorCode,\n" +
                "           tmp.IndicatorName,\n" +
                "           tmp.unified,\n" +
                "           tmp.product,\n" +
                "           tmp.BelongsArea,\n" +
                "           data.pubDate,\n" +
                "           data.measureValue,\n" +
                "           ROW_NUMBER() OVER (PARTITION BY tmp.IndicatorCode ORDER BY data.pubDate DESC) AS row_num\n" +
                "    FROM tmp\n" +
                "    JOIN data ON tmp.IndicatorCode = data.IndicatorCode" +
                ")\n" +
                "SELECT IndicatorCode,\n" +
                "           IndicatorName,\n" +
                "           unified,\n" +
                "           product,\n" +
                "           BelongsArea,\n" +
                "           pubDate,\n" +
                "           measureValue,\n" +
                "       row_num\n" +
                "FROM rank_Table ";
        sparkSession.sql(rankTableSql).createOrReplaceTempView("rank_Table");
//        sparkSession.sql(rankTableSql).show();
    }

    //  Return SQL query statement
    private static String getSql() {
        return "WITH latest_dates AS (\n" +
                "    SELECT IndicatorCode, pubDate as latest_date,measureValue as latest_measure_value\n" +
                "    FROM rank_table\n" +
                "    where row_num = 1\n" +
                "),\n" +
                "previous_year_dates AS (\n" +
                "    SELECT t1.IndicatorCode,\n" +
                "           t1.pubDate as previous_year_date,\n" +
                "           t1.measureValue as previous_year_measure_value\n" +
                "    FROM rank_table t1\n" +
                "             INNER JOIN latest_dates t2\n" +
                "                        ON t1.IndicatorCode = t2.IndicatorCode\n" +
                "    WHERE t1.pubDate = date_add(t2.latest_date, -365)\n" +
                "),\n" +
                "yoy_tabel as (SELECT t3.IndicatorCode,\n" +
                "                     t3.latest_date,\n" +
                "                     t4.previous_year_date,\n" +
                "                     t3.latest_measure_value,\n" +
                "                     t4.previous_year_measure_value,\n" +
                "                     (t3.latest_measure_value - t4.previous_year_measure_value) as measure_value_difference\n" +
                "              FROM latest_dates t3\n" +
                "                       INNER JOIN previous_year_dates t4\n" +
                "                                  ON t3.IndicatorCode = t4.IndicatorCode),\n" +
                "rise_fall_table as (select product                                         as product,\n" +
                "                           IndicatorCode                                   as indicator_code,\n" +
                "                           IndicatorName                                   as indicator_name,\n" +
                "                           BelongsArea                                     as address,\n" +
                "                           measureValue                                    as price,\n" +
                "                           previous_price                                  as previous_price,\n" +
                "                           (measureValue - previous_price)                 as rise_fall,\n" +
                "                           round((measureValue - previous_price) / COALESCE(NULLIF(previous_price, 0), 1) * 100, 6) as percentage,\n" +
                "                           pubDate                                         as `to_date`,\n" +
                "                           unified                                         as unit\n" +
                "                    from (select IndicatorCode,IndicatorName,unified,BelongsArea,measureValue,pubDate,product,row_num,\n" +
                "                                 LEAD(measureValue) OVER (PARTITION BY IndicatorCode ORDER BY pubDate desc) AS previous_price\n" +
                "                          from rank_table\n" +
                "                          WHERE measureValue is not null\n" +
                "                            and row_num <= 2) tmp1\n" +
                "                    where row_num = 1)\n" +
                "select\n" +
                "product,\n" +
                "indicator_code,\n" +
                "indicator_name,\n" +
                "address,\n" +
                "price,\n" +
                "previous_price,\n" +
                "rise_fall,\n" +
                "percentage,\n" +
                "to_date,\n" +
                "unit,\n" +
                "rise_fall as ring_ratio,\n" +
                "if(measure_value_difference is null, 0, measure_value_difference) as yoy\n" +
                "from rise_fall_table rft\n" +
                "         left join yoy_tabel yt\n" +
                "                   on rft.indicator_code = yt.IndicatorCode  ";
    }

}

