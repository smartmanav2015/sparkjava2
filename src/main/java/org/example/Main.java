package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.ReduceFunction;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class Main {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark Java 17 Demo")
                .master("local[*]")
                .getOrCreate();

        List<Integer> numbers = Arrays.asList(1,2,3,4,5);

        Dataset<Integer> ds =
                spark.createDataset(numbers, Encoders.INT());

        Integer sum = ds.reduce(
                (ReduceFunction<Integer>) Integer::sum
        );

        System.out.println("SUM = " + sum);
        log.info("#############################");
        while (true){
            log.info("Running...");
        }

        //spark.stop();
    }
}
