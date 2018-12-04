package net.jgp.books.sparkWithJava;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class InternationalLoansApp {

    public static void main(String[] args) {

        InternationalLoansApp app = new InternationalLoansApp();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("CSV to DataFrame")
                .master("local[*]")
                .getOrCreate();

        // Loads CSV file from Google Storage Bucket
        Dataset<Row> dfLoans = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", true)
                .load("gs://gstafford-spark-demo/ibrd-statement-of-loans-latest-available-snapshot.csv");

        System.out.printf("DataFrame Row Count: %d%n", dfLoans.count());
        dfLoans.printSchema();

        // Creates temporary view using DataFrame
        dfLoans.withColumnRenamed("Country", "country")
                .withColumnRenamed("Country Code", "country_code")
                .withColumnRenamed("Original Principal Amount", "principal")
                .withColumnRenamed("Interest Rate", "rate")
                .createOrReplaceTempView("loans");

        // Performs basic analysis of dataset
        Dataset<Row> dfPrincipal = spark.sql(
                "SELECT country, country_code, format_number(total_principal, 0) AS total_principal, " +
                        "format_number(avg_rate, 2) AS avg_rate " +
                        "FROM (" +
                        "SELECT country, country_code, SUM(principal) AS total_principal, AVG(rate) avg_rate " +
                        "FROM loans " +
                        "GROUP BY country " +
                        "ORDER BY total_principal DESC)"
        );

        dfPrincipal.show(10, 100);

        // Saves results to single CSV file in Google Storage Bucket
        dfPrincipal.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .option("header", "true")
                .save("gs://gstafford-spark-demo/ibrd-total-principal");
    }
}
