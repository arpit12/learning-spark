package org.learningspark.simple;

import com.google.common.base.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Line Count with Filtering of empty lines
 */
public class LineCountWithFiltering {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("File Copy");
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

    long count = nonEmptyLines.count();

    System.out.println(String.format("Total lines in %s is %d",args[0],count));
  }

}
