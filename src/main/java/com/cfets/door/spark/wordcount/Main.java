package com.cfets.door.spark.wordcount;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Hello Kitty!");
		if (args.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		// 初始化spark配置
		SparkConf conf = new SparkConf().setAppName("word count");
//		 conf.setMaster("local[2]");
		 conf.set("spark.executor.memory", "1g");
		 conf.set("spark.cores.max", "2");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile(args[0], 1);

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
					public Iterable<String> call(String s) throws Exception {
						return Arrays.asList(s.split(" ", -1));
					}
				});
		
		 JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			      public Tuple2<String, Integer> call(String s) {
			       return new Tuple2<String, Integer>(s, 1);
			      }
			    });
		 
		 JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			        public Integer call(Integer i1, Integer i2) {
			         return i1 + i2;
			        }
			     });
		 
		 counts.saveAsTextFile(args[1]);
		 

	}

}
