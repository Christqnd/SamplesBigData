/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mongodb.spark_examples;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 *
 * @author christ
 */
public class WordCountWithSpark {
    
    public static void main(String[] args) {
            SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("JavaWordCountWithSpark")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate(); //configuramos una session de spark

        JavaRDD<String> textFile = spark.read().textFile("hdfs://localhost:54310/ContadorPalabras/Input/letters").javaRDD();
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        //counts.saveAsTextFile("hdfs://localhost:54310/ContadorPalabras/Output/output2.txt"); //se guarda en directorio hdfs de hadoop
        List<Tuple2<String, Integer>> output = counts.collect(); //presenta en consola el resultado
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();
    }
        
}
