package br.ufc.br.questao2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.gson.Gson;

import br.ufc.br.questao2.model.Tweet;

public class ItemD {

	public static class CreatedAtByReviewsMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			Gson gson = new Gson();
			Tweet t = gson.fromJson(value.toString(), Tweet.class);

			String reviews = t.getAuthor().getReviews().replace(",", ".");
			String createdAt = (t.getCreatedAt());
			
			if (!reviews.isEmpty() && createdAt!=null) {
				Double numParsed = Double.parseDouble(reviews);
				Integer reviewsNumber = (int) Math.round(numParsed);
				context.write(new Text(createdAt), new IntWritable(reviewsNumber));
			}
		}
	}

	public static class CreatedAtByReviewsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
		Job job = Job.getInstance(conf, "item d");
		job.setJarByClass(ItemA.class);
		job.setMapperClass(CreatedAtByReviewsMapper.class);
		job.setCombinerClass(CreatedAtByReviewsReducer.class);
		job.setReducerClass(CreatedAtByReviewsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}