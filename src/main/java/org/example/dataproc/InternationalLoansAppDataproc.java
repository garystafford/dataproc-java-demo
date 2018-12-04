package org.example.dataproc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class InternationalLoansAppDataproc {

    public static void main(String[] args) {

        InternationalLoansAppDataproc app = new InternationalLoansAppDataproc();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("CSV to DataFrame")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN"); // INFO by default

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
                .withColumnRenamed("Disbursed Amount", "disbursed")
                .withColumnRenamed("Borrower's Obligation", "obligation")
                .withColumnRenamed("Interest Rate", "interest_rate")
                .createOrReplaceTempView("loans");

        // Performs basic analysis of dataset
        Dataset<Row> dfDisbursement = spark.sql(
                "SELECT country, country_code, " +
                        "format_number(total_disbursement, 0) AS total_disbursement, " +
                        "format_number(total_obligation, 0) AS total_obligation, " +
                        "format_number(avg_interest_rate, 2) AS avg_interest_rate " +
                        "FROM (" +
                        "SELECT country, country_code, " +
                        "SUM(disbursed) AS total_disbursement, " +
                        "SUM(obligation) AS total_obligation, " +
                        "AVG(interest_rate) avg_interest_rate " +
                        "FROM loans " +
                        "GROUP BY country, country_code " +
                        "ORDER BY total_disbursement DESC)"
        );

        dfDisbursement.show(10, 100);

        Dataset<Row> dfGrandTotalDisbursement = spark.sql(
                "SELECT format_number(SUM(disbursed),0) AS grand_total_disbursement FROM loans"
        );

        dfGrandTotalDisbursement.show();

        Dataset<Row> dfGrandTotalObligation = spark.sql(
                "SELECT format_number(SUM(obligation),0) AS grand_total_obligation FROM loans"
        );

        dfGrandTotalObligation.show();

        // Saves results to single CSV file in Google Storage Bucket
        dfDisbursement.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .option("header", "true")
                .save("gs://gstafford-spark-demo/ibrd-loan-summary");

        System.out.println("Results successfully written to CSV file");
    }
}
