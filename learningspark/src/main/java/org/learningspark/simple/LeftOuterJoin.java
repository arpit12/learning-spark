package org.learningspark.simple;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.StringReader;

/**
 * Left Outer Join Example
 */
public class LeftOuterJoin {
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("File Copy");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


    // Read the source file
    JavaRDD<String> adInput = sparkContext.textFile(args[0]);

    // Now we have non-empty lines, lets split them into words
    JavaPairRDD<String, String> adsRDD = adInput.mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) {
        CSVReader csvReader = new CSVReader(new StringReader(s));
        // lets skip error handling here for simplicity
        try {
          String[] adDetails = csvReader.readNext();
          return new Tuple2<String, String>(adDetails[0], adDetails[1]);
        } catch (IOException e) {
          e.printStackTrace();
          // noop
        }
        // Need to explore more on error handling
        return new Tuple2<String, String>("-1", "1");
      }
    });

    // Read the impressions
    JavaRDD<String> impressionInput = sparkContext.textFile(args[1]);

    // Now we have non-empty lines, lets split them into words
    JavaPairRDD<String, String> impressionsRDD = impressionInput.mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) {
        CSVReader csvReader = new CSVReader(new StringReader(s));
        // lets skip error handling here for simplicity
        try {
          String[] adDetails = csvReader.readNext();
          return new Tuple2<String, String>(adDetails[0], adDetails[1]);
        } catch (IOException e) {
          e.printStackTrace();
          // noop
        }
        // Need to explore more on error handling
        return new Tuple2<String, String>("-1", "1");
      }
    });


    // Lets go for an inner join, to hold data only for Ads which received an impression
    JavaPairRDD<String, Tuple2<String, Optional<String>>> joinedData = adsRDD.leftOuterJoin(impressionsRDD);


    joinedData.saveAsTextFile("./output-outerjoin");

  }
}
