package com.JLC.demo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

/**
 * DzProduce   com.JLC.demo
 * 2023-03-2023/3/30   14:27
 *
 * @author : zhangmingyue
 * @description :
 * @date : 2023/3/30 2:27 PM
 */
public class IndexUnifiedFormat {
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
        String sinkTable = "st_jlc_index";
        SparkSession sparkSession = SparkSession.builder()
                .appName("IndexUnifiedFormat")
                .master("local[*]")
                .getOrCreate();

        getDF(sparkSession, tidbUrl, tidbUser, tidbPassword, indexTable).createOrReplaceTempView("index");

        getTmpView(sparkSession);
        String getTmptable = "select i.id as IndicatorCode,\n" +
                "       i.name as IndicatorName,\n" +
                "        i.toDate as endDate,\n" +
                "       i.updField as measureFiled,\n" +
                "       i.attr as content,\n" +
                "       i.subCode as source,\n" +
                "       case\n" +
                "           when i.updFreq = 'HALF' then 'HALF MONTH'\n" +
                "           else i.updFreq\n" +
                "        end as upd_freq,\n" +
                "       case\n" +
                "           when u.unified like \"%,_\" then REPLACE(u.unified, ',', '/')\n" +
                "           when u.unified like \"_,\" then REPLACE(u.unified, ',', '')\n" +
                "           else u.unified\n" +
                "           end as unified\n" +
                "from index i\n" +
                "         left join Unit u on i.id = u.id";
        Dataset<Row> indexDF = sparkSession.sql(getTmptable);
        if (indexDF != null) {
            indexDF.show();
            writeToTiDB(indexDF, tidbUrl, tidbUser, tidbPassword, sinkTable);
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

    private static StructType createStructType() {
        return new StructType(new StructField[]{
                new StructField("attrField", DataTypes.StringType, true, Metadata.empty()),
                new StructField("attrName", DataTypes.StringType, true, Metadata.empty()),
        });
    }

    //  Get tmpView function
    private static void getTmpView(SparkSession sparkSession) {
        //  Get attr column
        String getAttrSql = "select distinct id, attr from index";
        Dataset<Row> attrData = sparkSession.sql(getAttrSql);
        // Define Array<StructType> schema
        DataType schema = DataTypes.createArrayType(createStructType());

        // Parse the attr column and extract each dictionary as a new row
        Dataset<Row> explodedDf = attrData.select(col("ID"), from_json(col("attr"), schema).as("attrArray"))
                .select(col("ID"), explode(col("attrArray")).as("attr"))
                .select(col("ID"), col("attr.attrField"), col("attr.attrName"));
        // Check whether the attrField column contains' Unit', to assign the value of the attrName column to the new unified column
        Dataset<Row> unifiedDf = explodedDf.withColumn("unified", when(col("attrField").contains("unit"), col("attrName")).otherwise(null));
        // Merge data so that there is only one row per ID // Create tmp view
        unifiedDf.groupBy("ID").agg(first("unified", true).as("unified")).createOrReplaceTempView("Unit");
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
