package com.JLC.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * DzProduce   com.JLC.demo
 * 2023-03-2023/3/30   14:28
 *
 * @author : zhangmingyue
 * @description : JLC treeData process
 * @date : 2023/3/30 2:28 PM
 */
public class TreeUnifiedFormat {
    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger logger = Logger.getLogger("SpzsIndex");
        logger.setLevel(Level.ERROR);
        //      read from configuration file, get configuration
        Properties prop = new Properties();
        InputStream inputStream = JLCAllData2Tidb.class.getClassLoader().getResourceAsStream("application.properties");
        prop.load(inputStream);

        String tidbUrl = prop.getProperty("tidb.url_warehouse");
        String tidbUser = prop.getProperty("tidb.user");
        String tidbPassword = prop.getProperty("tidb.password");

        String indexTable = "jlc_index";
        String treeTable = "jlc_tree";
        String sinkTable = "st_jlc_tree";

        SparkSession sparkSession = SparkSession.builder()
                .appName("TreeUnifiedFormat")
                .master("local[*]")
                .getOrCreate();

        getDF(sparkSession, tidbUrl, tidbUser, tidbPassword, indexTable).createOrReplaceTempView("index");
        getDF(sparkSession, tidbUrl, tidbUser, tidbPassword, treeTable).createOrReplaceTempView("tree");


        String treeChange = "select\n" +
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
        Dataset<Row> treeDF = sparkSession.sql(treeChange);

        if (treeDF != null) {
            treeDF.show();
//            writeToTiDB(treeDF, tidbUrl, tidbUser, tidbPassword, sinkTable);
        }
        sparkSession.stop();
    }

    private static Dataset<Row> getDF(SparkSession sparkSession, String url, String user, String password, String table) {
        return sparkSession.read()
                .format("jdbc")
                .option("url", url)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", table)
                .option("user", user)
                .option("password", password)
                .load().toDF();
    }

    private static void writeToTiDB(Dataset<Row> dataFrame, String url, String user, String password, String table) {
        dataFrame.repartition(10).write()
                .mode(SaveMode.Append)
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", url)
                .option("user", user)
                .option("password", password)
                .option("dbtable", table)
                .option("isolationLevel", "NONE")    //不开启事务
                .option("batchsize", 10000)   //设置批量插入
                .save();
    }
}
