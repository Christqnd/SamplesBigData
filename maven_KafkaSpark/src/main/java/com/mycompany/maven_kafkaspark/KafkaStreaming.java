/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.maven_kafkaspark;

import io.netty.handler.codec.string.StringDecoder;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.streaming.Durations;
import org.apache.spark.SparkConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;


/**
 *
 * @author christ
 */
public class KafkaStreaming{

    static Map<String, Object> kafkaParams = new HashMap<>();

    public static void main(String[] args){
        try {
            // Create a local StreamingContext with two working thread and batch interval of 1 second
            SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Sampleapp").set("spark.driver.bindAddress", "localhost");
            JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
            
            kafkaParams.put("bootstrap.servers", "localhost:9092");
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", StringDeserializer.class);
            kafkaParams.put("group.id", "0");
            kafkaParams.put("auto.offset.reset", "earliest"); // from-beginning?
            kafkaParams.put("enable.auto.commit", false);
            
            Collection<String> topics = Arrays.asList("test");
            
            final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                            jssc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                    );
            
            JavaPairDStream<String, String> jPairDStream = stream.mapToPair(
                    new PairFunction<ConsumerRecord<String, String>, String, String>() {
                        @Override
                        public Tuple2<String, String> call(ConsumerRecord<String, String> record) throws Exception {
                            return new Tuple2<>(record.key(), record.value());
                        }   
                    });
            
//            JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(
//                    ssc,String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
            
            jPairDStream.foreachRDD(jPairRDD -> {
                System.out.println("--- Nuevo RDD con " + jPairRDD.partitions().size() + " particiones and " + jPairRDD.count() + " registros");
                jPairRDD.foreach(rdd -> {System.out.println("key=" + rdd._1() + " value=" + rdd._2());});
            });
            
            jssc.start();
            
            jssc.awaitTermination();
            
//            System.out.println("Direct Stream created? ");
//            stream.mapToPair(
//                    new PairFunction<ConsumerRecord<String, String>, String, String>() {
//                        @Override
//                        public Tuple2<String, String> call(ConsumerRecord<String, String> record) throws Exception {
//                            System.out.println("record key : " + record.key() + " value is : " + record.value());
//                            return new Tuple2<>(record.key(), record.value());
//                        }
//                    });
//            
//            System.out.println("Reached the end.");
            
        } catch (InterruptedException ex) {
            Logger.getLogger(KafkaStreaming.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
