package org.learningspark.simple;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.StringReader;

/**
 * Simple Aggregation
 */
public class SimpleAggregation {
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("Ad Provider Aggregation");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


    // Read the source file
    JavaRDD<String> adInput = sparkContext.textFile(args[0]);

    // Now we have non-empty lines, lets split them into words
    JavaPairRDD<String, Integer> adsRDD = adInput.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) {
        CSVReader csvReader = new CSVReader(new StringReader(s));
        // lets skip error handling here for simplicity
        try {
          String[] adDetails = csvReader.readNext();
          return new Tuple2<String, Integer>(adDetails[1], 1);
        } catch (IOException e) {
          e.printStackTrace();
          // noop
        }
        // Need to explore more on error handling
        return new Tuple2<String, Integer>("-1", 1);
      }
    });

    JavaPairRDD<String, Integer> adsAggregated = adsRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer integer, Integer integer2) throws Exception {
        return integer + integer2;
      }
    });

    adsAggregated.saveAsTextFile("./output/ads-aggregated-provider");
  }
}
