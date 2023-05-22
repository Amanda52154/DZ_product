package com.Test.demo;


import com.JLC.demo.ApiHelper;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;


/**
 * DzProduce   com.JLC.demo
 * 2023-03-2023/3/30   14:28
 *
 * @author : zhangmingyue
 * @description : JLC treeData process
 * @date : 2023/3/30 2:28 PM
 */
public class TreeUnifiedFormat extends ApiHelper {
    public TreeUnifiedFormat(String apiUrl) {
        super(apiUrl);
    }

    public static void main(String[] args) throws IOException {
        String appName = "TreeUnifiedFormat";
        SparkSession sparkSession = defaultSparkSession(appName);

        String indexTable = "jlc_index";
        String treeTable = "jlc_tree";
        String sinkTable = "st_jlc_tree";

        getDF(sparkSession, indexTable).createOrReplaceTempView("index");
        getDF(sparkSession, treeTable).createOrReplaceTempView("tree");

        writeToTiDB(sparkSession.sql(getSql()), sinkTable);
        sparkSession.stop();
    }
    private static String getSql(){
        return "select\n" +
                "t.id as treeID,\n" +
                "t.pId as PID,\n" +
                "t.name as NodeName,\n" +
                "t.idPath as pathId,\n" +
                "t.namePath as pathName,\n" +
                "t.updField as upd_field,\n" +
                "i.fromDate as from_date,\n" +
                "t.subCode as sub_code,\n" +
                "t.updFreq as upd_freq,\n" +
                "t.category as category,\n" +
                "i.toDate as to_date\n" +
                "from tree t left join index i on t.id = i.id";
    }
}
