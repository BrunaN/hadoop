package br.ufc.br.questao1;

import java.io.IOException;
import java.util.Iterator;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ordenador {
	public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, OutputCollector<IntWritable, Text> collector, Reporter arg3)
				throws IOException {
			String line = value.toString();
			StringTokenizer stringTokenizer = new StringTokenizer(line);
			{
				int number = 999;
				String word = "empty";

				if (stringTokenizer.hasMoreTokens()) {
					String str0 = stringTokenizer.nextToken();
					word = str0.trim();
				}

				if (stringTokenizer.hasMoreElements()) {
					String str1 = stringTokenizer.nextToken();
					number = Integer.parseInt(str1.trim());
				}

				collector.collect(new IntWritable(number), new Text(word));
			}

		}

	}

	public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> arg2,
				Reporter arg3) throws IOException {
			while ((values.hasNext())) {
				arg2.collect(key, values.next());
			}

		}

	}



	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "questao ");
		job.setJarByClass(Ordenador.class);
		job.setMapperClass(Mapper.class);
		job.setCombinerClass(Reducer.class);
		job.setReducerClass(Reducer.class);
//		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);


		FileInputFormat.addInputPath(job,
				new Path("/home/igor/projetos/pessoal/hadoop/Hadoop/src/resources/databases/output/q1/letra_a/manha"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/home/igor/projetos/pessoal/hadoop/Hadoop/src/resources/databases/output/q1/letra_a/final/manha"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
