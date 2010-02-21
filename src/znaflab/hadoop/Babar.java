package org.znaflab;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Babar {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, BytesWritable> {
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String url = value.toString();
		HttpClient client = new HttpClient();
		GetMethod method = new GetMethod(url);
		
		// Send GET request
		int statusCode = client.executeMethod(method);
		
		java.io.BufferedInputStream in = new java.io.BufferedInputStream(new java.net.URL(url).openStream());
		java.io.ByteArrayOutputStream bout = new ByteArrayOutputStream();
		byte buffer[] = new byte[1024 * 1024];
		
		while( in.read(buffer, 0, buffer.size) >= 0 ) {
			bout.write(buffer);
		}
		
		collect(value, new BytesWritable(bout.toByteArray()));
		in.close();
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Babar.class);
		conf.setJobName("babar");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		}
}
