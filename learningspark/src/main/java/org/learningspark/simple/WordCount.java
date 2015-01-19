package org.learningspark.simple;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Famous WordCount example
 */
public class WordCount {
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("Word Count");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


    // Read the source file
    JavaRDD<String> input = sparkContext.textFile(args[0]);

    // RDD is immutable, let's create a new RDD which doesn't contain empty lines
    // the function needs to return true for the records to be kept
    JavaRDD<String> nonEmptyLines = input.filter(new Function<String, Boolean>() {
      @Override
      public Boolean call(String s) throws Exception {
        if(s == null || s.trim().length() < 1) {
          return false;
        }
        return true;
      }
    });

    // Now we have non-empty lines, lets split them into words
    JavaRDD<String> words = nonEmptyLines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String s) throws Exception {
        return Arrays.asList(s.split(" "));
      }
    });

    // Convert words to Pairs, remember the TextPair class in Hadoop world
    JavaPairRDD<String, Integer> wordPairs = words.mapToPair(new PairFunction<String, String, Integer>() {
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<String, Integer>(s, 1);
      }
    });

    JavaPairRDD<String, Integer> wordCount = wordPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer integer, Integer integer2) throws Exception {
        return integer + integer2;
      }
    });

    // Just for debugging, NOT FOR PRODUCTION
    wordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
      @Override
      public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        System.out.println(String.format("%s - %d", stringIntegerTuple2._1(), stringIntegerTuple2._2()));
      }
    });

  }
}
