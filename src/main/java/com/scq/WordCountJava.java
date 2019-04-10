package com.scq;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCountJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("wordcountjava").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        JavaRDD<String> lines = sc.textFile("D:/test/test/b.txt");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> asList = Arrays.asList(s.split(" "));
                return asList.iterator();
            }
        });
        JavaPairRDD<String, Integer> word = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
        JavaPairRDD<String, Integer> result = word.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<Integer, String> newresult = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<Integer, String>(tuple2._2, tuple2._1);
            }
        });

        JavaPairRDD<Integer, String> sortresult = newresult.sortByKey(false);
        JavaPairRDD<String, Integer> reresult = sortresult.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return new Tuple2<String, Integer>(tuple2._2, tuple2._1);
            }
        });

        System.out.println(reresult.collect());
        sc.stop();
    }
}
