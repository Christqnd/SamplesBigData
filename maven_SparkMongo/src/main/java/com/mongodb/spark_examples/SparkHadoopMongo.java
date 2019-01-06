/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mongodb.spark_examples;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.Document;
import scala.Tuple2;

/**
 *
 * @author christ
 */
public class SparkHadoopMongo {

    static String fileName = "hdfs://localhost:54310/tests/clientes"; //localizado en el sistema de archivos hdfs de hadoop
    static String[] schema = {"cod", "nombre", "apellidos", "valor", "oficio"}; //estructura de nuestra coleccion en la base de datos

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkToMongoDB").set("spark.driver.bindAddress", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(fileName);

        JavaRDD<Map<String, ?>> fields = textFile
                .map(new Function<String, Map<String, ?>>() {
                    private static final long serialVersionUID = 1L;

                    public Map<String, ?> call(String line) throws Exception {
                        Map<String, String> mongodbFields = new HashMap<String, String>();
                        String fieldSplit[] = line.split(",");

                        for (int i = 1; i < fieldSplit.length; i++) {
                            mongodbFields.put(schema[i], fieldSplit[i]);

                        }

                        return mongodbFields;
                    }
                });

        JavaPairRDD<String, HashMap<String, Object>> rddFields = textFile
                .mapToPair(new PairFunction<String, String, HashMap<String, Object>>() {
                    private static final long serialVersionUID = 1L;

                    public Tuple2<String, HashMap<String, Object>> call(String s) {
                        HashMap<String, Object> values = new HashMap<String, Object>();
                        String lineValues[] = s.split(",");
                        String id = "";
                        for (int i = 0; i < schema.length; i++) {
                            if (i < lineValues.length) {
                                values.put(schema[i], lineValues[i]);
                            }
                        }
                        id = lineValues[0];
                        return new Tuple2<String, HashMap<String, Object>>(id,
                                values);
                    }
                });

        JavaRDD<Document> mongodbDocuments = rddFields.values()
                .map(
                        new Function<HashMap<String, Object>, Document>() {
                    private static final long serialVersionUID = 1L;

                    public Document call(HashMap<String, Object> arg0) throws Exception {
                        Document mongodbDoc = new Document();
                        mongodbDoc.putAll(arg0);
                        return mongodbDoc;
                    }
                });
        try {
            MongoSpark.save(mongodbDocuments, getConfig());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        sc.close();
    }

    private static WriteConfig getConfig() {
        Map<String, String> configMongoDB = new HashMap<String, String>();
        configMongoDB.put("spark.mongodb.output.uri", "mongodb://localhost/test.spark");
        WriteConfig.create(configMongoDB);
        WriteConfig writeConfig = WriteConfig.create(configMongoDB);
        return writeConfig;
    }
}
