package com.ZJS.demo;

import com.JLC.demo.ApiHelper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;

/**
 * DzProduce   com.ZJS.demo
 * 2023-03-2023/3/25   12:13
 *
 * @author : zhangmingyue
 * @description : ZJS data Process
 * @date : 2023/3/21 12:13 PM
 */



public class ZJSUnifiedData extends ApiHelper {
    public ZJSUnifiedData(String apiUrl) {
        super(apiUrl);
    }

    public static void main(String[] args) throws IOException {

        String appName = "ZJSUnifiedData";
        SparkSession sparkSession = defaultSparkSession(appName);

        String indicatordatavTable = "c_in_indicatordatav";/*"(select * from c_in_indicatordatav where zjs_update_time >= '2023-03-30') t";*/
        String indicatormainTable = "c_in_indicatormain";
        String dictionaryTable = "c_in_dictionary";
        String sinkTable = "st_c_in_indicatordatav";

        getDF(sparkSession, indicatordatavTable).createOrReplaceTempView("indicatordatav");
        getDF(sparkSession, dictionaryTable).createOrReplaceTempView("dictionary");
        getDF(sparkSession, indicatormainTable).createOrReplaceTempView("indicatormain");

        Dataset<Row> tidbDF = sparkSession.sql(getSql());
        tidbDF.show();
//        writeToTiDB(tidbDF, sinkTable);

        sparkSession.stop();
    }
    private static String getSql(){
        return "select data.ID,\n" +
                "       data.IndicatorCode,\n" +
                "       data.InfoPublDate,\n" +
                "       data.BeginDate,\n" +
                "       if(data.EndDate = '1969-12-31 00:00:00' , '1970-01-01 08:00:01',data.EndDate) as EndDate,\n" +
                "       if(dic.coefficient is not null, data.DataValue * dic.coefficient, data.DataValue) as DataValue,\n" +
                "       data.PowerNumber,\n" +
                "       data.UpdateTime,\n" +
                "       data.JSID,\n" +
                "       data.zjs_insert_time,\n" +
                "       data.zjs_update_time\n" +
                "from indicatordatav data\n" +
                "         left join\n" +
                "     indicatormain main on data.IndicatorCode = main.IndicatorCode\n" +
                "         left join dictionary dic on main.UnitCode = dic.UnitCode";
    }
}
