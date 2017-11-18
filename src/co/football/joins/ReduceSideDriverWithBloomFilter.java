package co.football.joins;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.log4j.BasicConfigurator;

public class ReduceSideDriverWithBloomFilter {

	public static void main(String[] args) throws Exception {

		BasicConfigurator.configure();
		if(args.length!=4) {
			System.err.println("Usage BloomFilterDriver: <bloom filter> <champions data set> <football data set> <output>");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Reduce side data procces");
		job.setJarByClass(co.football.joins.ReduceSideDriverWithBloomFilter.class);
		job.addCacheFile(new Path(args[0]).toUri());
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ChampReduceSideMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, FootReduceSideMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(ReduceSideReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		if (!job.waitForCompletion(true))
			return;
	}

	public static class ChampReduceSideMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
			String[] split = ivalue.toString().split(",");
			String country = split[0];
			int value = Integer.parseInt(split[1]);
			context.write(new Text(country), new IntWritable(value));
		}

	}

	public static class FootReduceSideMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		Pattern p = Pattern.compile("^[A-Z].*$");
		private BloomFilter filter = new BloomFilter();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			Path path = Job.getInstance(context.getConfiguration()).getLocalCacheFiles()[0];
			FileSystem fs = FileSystem.newInstance(context.getConfiguration());
			FSDataInputStream open = fs.open(path);
			filter.readFields(open);
		}

		public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
			String[] strings = ivalue.toString().split(",");
			String string = strings[11];
			Matcher m = p.matcher(string);
			if (m.matches() && filter.membershipTest(new Key(string.getBytes()))) {
				context.write(new Text(string), new IntWritable(10));
			}

		}

	}

	public static class ReduceSideReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text _key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			boolean print = false;
			int count = 0;
			for (IntWritable v : values) {
				count += v.get();
				if (v.get() != 10) {
					count -= v.get();
					print = true;
				}
			}
			if (print) {
				context.write(_key, new IntWritable(count / 10));
			}
		}

	}

}
