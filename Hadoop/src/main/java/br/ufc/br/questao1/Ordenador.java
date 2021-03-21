package br.ufc.br.questao1;

import java.io.IOException;

import java.util.List;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ordenador {

	public static class ReviewsMapper extends Mapper<Object, Text, IntWritable, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String text = value.toString();
			List<String> tokens = Arrays.asList(text.split("\t"));
			String counter = tokens.get(1);

			context.write(new IntWritable(Integer.parseInt(counter)), new Text(tokens.get(0)));
		}
	}

	public static class DescendingIntWritableComparable extends IntWritable {
		/** A decreasing Comparator optimized for IntWritable. */
		public static class DecreasingComparator extends Comparator {
			public int compare(WritableComparable a, WritableComparable b) {
				return -super.compare(a, b);
			}

			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
				return -super.compare(b1, s1, l1, b2, s2, l2);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ordenador");
		job.setJarByClass(Ordenador.class);
		job.setMapperClass(ReviewsMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setSortComparatorClass(DescendingIntWritableComparable.DecreasingComparator.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}