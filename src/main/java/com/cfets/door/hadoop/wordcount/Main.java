package com.cfets.door.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Hello world!
 *
 */
public class Main {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		System.out.println("Hello Kitty!");
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 2) {

			System.err.println("Usage: wordcount <in> <out>");

			System.exit(2);

		}

		Job job = new Job(conf, "word count");

		job.setNumReduceTasks(1);

		job.setJarByClass(Main.class);

		job.setMapperClass(WordCountMapper.class);

		job.setCombinerClass(WordCountReducer.class);

		job.setReducerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
