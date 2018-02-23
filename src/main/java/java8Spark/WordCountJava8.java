package java8Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by gongwenzhou on 2018/2/23.
 */
public class WordCountJava8 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCountJava8");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile("d:\\wordCount.txt", 2)
                .flatMap(line -> Arrays.asList(line.split("\t")).iterator())
                .mapToPair(word -> new Tuple2<String, Integer>(word,1))
                .reduceByKey((m,n) -> m + n)
                .mapToPair(tuple->new Tuple2<Integer, String>(tuple._2,tuple._1))
                .sortByKey(false)
                .foreach(t-> System.out.println(t._2 + " -> " + t._1));

        sc.close();


    }

}
