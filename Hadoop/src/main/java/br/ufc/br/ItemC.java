package br.ufc.br;
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

import br.ufc.br.model.Tweet;

public class ItemC {

	public static class ReviewsMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			Gson gson = new Gson();
			Tweet t = gson.fromJson(value.toString(), Tweet.class);

			String reviews = t.getAuthor().getReviews().replace(",", "");
			String id = t.getId();
			
			if(!reviews.isEmpty())
				context.write(new Text(id), new IntWritable(Integer.parseInt(reviews)));
						
		}
	}

	public static class ReviewsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {		
		public void reduce(Text key, IntWritable values, Context context)
				throws IOException, InterruptedException {
			context.write(key, values);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "item c");
		job.setJarByClass(ItemA.class);
		job.setMapperClass(ReviewsMapper.class);
		job.setCombinerClass(ReviewsReducer.class);
		job.setReducerClass(ReviewsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}