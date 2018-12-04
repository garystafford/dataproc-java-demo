package net.jgp.books.sparkWithJava;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Multi-line ingestion JSON ingestion in a DataFrame.
 *
 * original source code author: https://github.com/jgperrin
 */
public class MultilineJsonToDataframeApp {

    public static void main(String[] args) {

        MultilineJsonToDataframeApp app = new MultilineJsonToDataframeApp();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Multi-line JSON to DataFrame")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("json")
                .option("multiline", true)
                .load("gs://gstafford-spark-demo/countrytravelinfo.json");

        df.printSchema();

        df.createOrReplaceTempView("countrytravelinfo");

        Dataset<Row> sqlDF = spark.sql(
                "SELECT geopoliticalarea, safety_and_security " +
                        "FROM countrytravelinfo " +
                        "WHERE lower(safety_and_security) LIKE '%terrorist%' " +
                        "ORDER BY geopoliticalarea"
        );
        sqlDF.show(10, 120);
    }
}