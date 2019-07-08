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
        //Create a SparkContext to initialize across cores on local machine
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Word Count");

        // Create a Java  Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inFile = "src/main/resources/shakespeare.txt";
        String outFile = "/tmp/shakespeareWordCount";
        // Load the text into a Spark RDD, which is a distributed representation of each line of text
        JavaRDD<String> textFile = sc.textFile(inFile);
        //JavaRDD<String> textFile = sc.textFile("hdfs:///tmp/shakespeare.txt");
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("[ ,]"))
                        .iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        //p just contains each a map of each Word as a key, with # appearances in text
        counts.foreach(p -> System.out.println(p));

        System.out.println("Total words: " + counts.count());

        //counts.saveAsTextFile("hdfs:///tmp/shakespeareWordCount");
        File writeFile = new File(outFile);
        if(!writeFile.exists()) {
            counts.saveAsTextFile("C:/Program_Codes/BigSpark"+outFile);
        }
    }
}
