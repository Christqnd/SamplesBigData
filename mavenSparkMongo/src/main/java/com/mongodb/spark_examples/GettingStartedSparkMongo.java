/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mongodb.spark_examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


/**
 *
 * @author hduser
 */
public class GettingStartedSparkMongo {

    public static void main(final String[] args) throws InterruptedException {
        /* Create the SparkSession.
     * If config arguments are passed from the command line using --conf,
     * parse args for the values to set.
         */
  
        SparkConf conf = new SparkConf().setMaster("local").setAppName("MiApp").set("spark.driver.bindAddress", "127.0.0.1");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        jsc.close();
       
    }
    
}
