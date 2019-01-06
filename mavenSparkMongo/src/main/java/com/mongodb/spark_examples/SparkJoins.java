/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mongodb.spark_examples;

import java.io.FileNotFoundException;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author christ
 */
public class SparkJoins {

    public static void main(String[] args) throws FileNotFoundException {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count").setMaster("local").set("spark.driver.bindAddress", "127.0.0.1"));
        JavaRDD<String> customerInputFile = sc.textFile("customers_data.txt");
        JavaPairRDD<String, String> customerPairs = customerInputFile.mapToPair(
                new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] customerSplit = s.split(",");
                return new Tuple2<String, String>(customerSplit[0], customerSplit[1] + " " + customerSplit[2]);
            }
        }
        ).distinct();

        JavaRDD<String> transactionInputFile = sc.textFile("transactions_data.txt");
        JavaPairRDD<String, String> transactionPairs = transactionInputFile.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] transactionSplit = s.split(",");
                return new Tuple2<String, String>(transactionSplit[2], transactionSplit[3] + "," + transactionSplit[1]);
            }
        });

        //Default Join operation (Inner join)
        JavaPairRDD<String, Tuple2<String, String>> joinsOutput = customerPairs.join(transactionPairs).sortByKey();
        List<Tuple2<String, Tuple2<String, String>>> output = joinsOutput.collect(); //presenta en consola el resultado
        System.out.println("******* Joins function Output : " + output);
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        /*
        //Left Outer join operation
        JavaPairRDD<String, Iterable<Tuple2<String, Optional<String>>>> leftJoinOutput = customerPairs.leftOuterJoin(transactionPairs).groupByKey().sortByKey();
        System.out.println("******* LeftOuterJoins function Output : " + leftJoinOutput.collect());

        //Right Outer join operation
        JavaPairRDD<String, Iterable<Tuple2<Optional<String>, String>>> rightJoinOutput = customerPairs.rightOuterJoin(transactionPairs).groupByKey().sortByKey();
        System.out.println("******* RightOuterJoins function Output : " + rightJoinOutput.collect());
         */
        sc.close();
    }

}
