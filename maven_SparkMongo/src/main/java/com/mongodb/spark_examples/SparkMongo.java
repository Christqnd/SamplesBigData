/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mongodb.spark_examples;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author christ
 */
public class SparkMongo {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/mydb.myCollection")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/mydb.myCollection")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // More application logic would go here...
        jsc.close();

    }

}
