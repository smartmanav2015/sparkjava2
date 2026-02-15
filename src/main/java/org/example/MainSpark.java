package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.ReduceFunction;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class MainSpark {

    public static void main(String[] args) throws InterruptedException {

        SparkSession spark = SparkSession.builder()
                .appName("Spark Java 17 Demo")
                .master("local[*]")
                .getOrCreate();

        List<Integer> numbers = Arrays.asList(1,2,3,4,5);
        log.info("#############################");
        while (true){
            Thread.sleep(1000L);
            log.info("Running...");
            Integer sum = getSum(spark, numbers);

            System.out.println("SUM = " + sum);
        }

        //spark.stop();
    }

    private static Integer getSum(SparkSession spark, List<Integer> numbers) {
        Dataset<Integer> ds =
                spark.createDataset(numbers, Encoders.INT());

        Integer sum = ds.reduce(
                (ReduceFunction<Integer>) Integer::sum
        );
        return sum;
    }
}
