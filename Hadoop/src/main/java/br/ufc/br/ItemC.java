package br.ufc.br;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.gson.Gson;

import br.ufc.br.model.Tweet;

public class ItemC {

	public static class ReviewsMapper extends Mapper<Object, Text, IntWritable, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			Gson gson = new Gson();
			Tweet t = gson.fromJson(value.toString(), Tweet.class);

			String reviews = t.getAuthor().getReviews().replace(",", ".");
			String id = t.getId();
			
			if(!reviews.isEmpty()) {
				Double numParsed = Double.parseDouble(reviews);
				Integer reviewsNumber = (int) Math.round(numParsed);
				context.write(new IntWritable(reviewsNumber), new Text(id));
			}
						
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
		Job job = Job.getInstance(conf, "item c");
		job.setJarByClass(ItemA.class);
		job.setMapperClass(ReviewsMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setSortComparatorClass(DescendingIntWritableComparable.DecreasingComparator.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}