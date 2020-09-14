package cs523.SparkWC;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWordCountjdk8
{

	public static void main(String[] args) throws Exception
	{
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

		//Threshold
		int threshold = Integer.parseInt(args[2]);
		
		// Load our input data
		JavaRDD<String> lines = sc.textFile(args[0])
					.map(w->w.toLowerCase());
		
		JavaPairRDD<String, Integer> eachWord = lines
					.flatMap(line -> Arrays.asList(line.split("\\W")))
					.mapToPair(w -> new Tuple2<String, Integer>(w, 1));
		
		// Calculate word count
		JavaPairRDD<String, Integer> counts = eachWord
					.reduceByKey((x, y) -> x + y);
		
		//Filter words that counts are greater than threshold
		JavaPairRDD<String, Integer> wordsGreaterThanTreshold = counts
				.filter(w -> w._2 > threshold);
		
		//Filter words that counts are less than threshold
		JavaPairRDD<String, Integer> less = counts
				.filter(w -> w._2 <= threshold);
				
		JavaRDD<String> allWords = eachWord.keys();
		JavaRDD<String> filteredWords = less.keys();
		JavaRDD<String> remainWords = allWords.subtract(filteredWords);
		
		JavaPairRDD<String, Integer> lettersCount = remainWords
				.flatMap(w -> Arrays.asList(w.split("")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y)
				.sortByKey();
		
		// Save the word count back out to a text file, causing evaluation
		counts.saveAsTextFile(args[1] + "/counts");
		wordsGreaterThanTreshold.saveAsTextFile(args[1] + "/wordsLenGteThreshold");
		lettersCount.saveAsTextFile(args[1] + "/lettersCount");

		sc.close();
	}
}
