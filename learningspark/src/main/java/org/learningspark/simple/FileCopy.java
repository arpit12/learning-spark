package org.learningspark.simple;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * File Copy program
 */
public class FileCopy {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("File Copy");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    JavaRDD<String> input = sparkContext.textFile(args[0]);

    input.saveAsTextFile(args[1]);
  }

}
