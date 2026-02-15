package com.deltalake;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

@Slf4j
public class MainDeltaLake {

    public static void main(String[] args) throws InterruptedException {

        SparkSession spark = SparkSession.builder()
                .appName("DeltaExperiment")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();


        String deltaTablePath = "e:\\delta-table";
        // Write data
        Dataset<Row> df = spark.range(0, 5).toDF();
        df.write().format("delta").save(deltaTablePath);

        log.info("Data written to Delta Lake at path: " + deltaTablePath);

        // Read data
        Dataset<Row> deltaTable = spark.read().format("delta").load(deltaTablePath);
        deltaTable.show();

        log.info("#############################");
        while (true){
            Thread.sleep(10000L);
            log.info("Running...");
        }

        //spark.stop();
    }
}
