package br.ufc.br.questao1;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
//import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ItemC {
	public static Reader getReader(String relativePath) throws UnsupportedEncodingException {

		return new InputStreamReader(ItemATarde.class.getResourceAsStream(relativePath), "UTF-8");

	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
//		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			List<String> tokens = Arrays.asList(line.split("\t"));

			String day = tokens.get(7).split(" ")[2];
			String hour = tokens.get(7).split(" ")[3].split(":")[0];
			
			context.write(new Text(day + "/" + hour), new IntWritable(1));
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);

			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "questao 2");
		job.setJarByClass(ItemC.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/home/igor/projetos/pessoal/databases/q1"));
		FileOutputFormat.setOutputPath(job,
				new Path("/home/igor/projetos/pessoal/databases/output/q1/letra_c/"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
