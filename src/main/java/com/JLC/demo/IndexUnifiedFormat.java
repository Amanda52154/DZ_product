package com.JLC.demo;


import org.apache.spark.sql.SparkSession;
import java.io.IOException;


import static org.apache.spark.sql.functions.*;

/**
 * DzProduce   com.JLC.demo
 * 2023-03-2023/3/30   14:27
 *
 * @author : zhangmingyue
 * @description : Process index unified
 * @date : 2023/3/30 2:27 PM
 */
public class IndexUnifiedFormat extends ApiHelper {
    public IndexUnifiedFormat(String apiUrl) {
        super(apiUrl);
    }

    public static void main(String[] args) throws IOException {

        String appName = "IndexUnifiedFormat";
        SparkSession sparkSession = defaultSparkSession(appName);

        String indexTable = "jlc_index";
        String sinkTable = "st_jlc_index";

        getDF(sparkSession, indexTable).createOrReplaceTempView("index");

        writeToTiDB(sparkSession.sql(getSql()), sinkTable);
        sparkSession.stop();
    }
    /*private static StructType createStructType() {
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
        Dataset<Row> unifiedDf = explodedDf.withColumn("unified", when(lower(col("attrField")).contains("unit"), col("attrName")).otherwise(null))
                .withColumn("product", when(lower(col("attrField")).contains("product"), col("attrName")).otherwise(null));
        // Merge data so that there is only one row per ID
        // Create tmp view
        unifiedDf.createOrReplaceTempView("temp_unifiedDf");

        // Use the temp view to group by ID and aggregate the unified and product columns
        sparkSession.sql("SELECT ID, first(unified, true) AS unified, first(product, true) AS product FROM temp_unifiedDf GROUP BY ID").createOrReplaceTempView("Unit");

    }*/
    private static String getSql(){
        String structSchema = "array<struct<attrField:string,attrName:string>>";
        return  "WITH attrData AS (SELECT DISTINCT id, attr FROM index),\n" +
                "explodedDf AS (SELECT ID, e.attrArray.attrField AS attrField, e.attrArray.attrName AS attrName\n" +
                "FROM (SELECT ID, from_json(attr, '" + structSchema + "') as attrArray FROM attrData) t\n" +
                "LATERAL VIEW explode(t.attrArray) e AS attrArray),\n" +
                "unifiedDf AS (SELECT ID,\n" +
                "first(CASE WHEN lower(attrField) LIKE '%unit%' THEN attrName END, true) AS unified,\n" +
                "first(CASE WHEN lower(attrField) LIKE '%product%' THEN attrName END, true) AS product\n" +
                "FROM explodedDf\n" +
                "GROUP BY ID)\n" +
                "SELECT i.id AS IndicatorCode, i.name AS IndicatorName, i.toDate AS endDate, i.updField AS measureFiled, i.attr AS content, i.subCode AS source,\n" +
                "CASE WHEN i.updFreq = 'HALF' THEN 'HALF MONTH' ELSE i.updFreq END AS upd_freq,\n" +
                "CASE\n" +
                "WHEN u.unified LIKE '%,%' THEN REPLACE(u.unified, ',', '/')\n" +
                "WHEN u.unified LIKE '%,%' THEN REPLACE(u.unified, ',', '')\n" +
                "ELSE u.unified\n" +
                "END AS unified\n" +
                "FROM index i LEFT JOIN (SELECT ID, unified FROM unifiedDf) u ON i.id = u.ID";

    }
}
