package com.rick;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import java.util.Arrays;

public class GoSpark {
	
	public GoSpark(){
//		SparkConf conf = new SparkConf().setAppName("rick").setMaster("local[*]")
//				.set("spark.ui.port", "4050");
//        SparkConf conf = new SparkConf();
//        conf.setMaster("local[1]");
        SparkSession session = SparkSession.builder().appName("rick").config("spark.master","spark://10.1.21.12:7077").getOrCreate();

//		JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Row> lines = session.read().text("D:\\test.txt").javaRDD();
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.getString(0).split(" ")).iterator());
        System.out.println(lines.count());
        System.out.println(words.count());
        //		JavaRDD<String> lines = sc.textFile("D\\test.txt");
//		JavaRDD<Integer> lineLengths = lines.map(s->s.length());
//		System.out.println("lines : "+lineLengths);
//		int total = lineLengths.reduce((a,b)->a+b);
//		System.out.println(total);
//		sc.close();

	}
	
}
