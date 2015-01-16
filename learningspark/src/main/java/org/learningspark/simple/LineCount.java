package org.learningspark.simple;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Line Count example
 */
public class LineCount {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("File Copy");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


    // Read the source file
    JavaRDD<String> input = sparkContext.textFile(args[0]);

    // Gets the number of entries in the RDD
    long count = input.count();

    System.out.println(String.format("Total lines in %s is %d",args[0],count));
  }

}
