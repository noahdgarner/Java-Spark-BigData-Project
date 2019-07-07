import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;

public class Main {

    public static void main(String[] args){
        //uncomment this if you want a ton of log errors to display in the console
        Logger.getLogger("org").setLevel(Level.ERROR);
        //so we can have our hadoop path set up. Download hadoop and set this path if needed
        System.setProperty("hadoop.home.dir", "C:\\winutil");
        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the text into a Spark RDD, which is a distributed representation of each line of text
        JavaRDD<String> textFile = sc.textFile("src/main/resources/shakespeare.txt");
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.foreach(p -> System.out.println(p));
        System.out.println("Total words: " + counts.count());
        File writeFile = new File("/tmp/shakespeareWordCount");
        if(!writeFile.exists()) {
            counts.saveAsTextFile("C:/Program_Codes/BigSpark/tmp/shakespeareWordCount");
        }
    }

}