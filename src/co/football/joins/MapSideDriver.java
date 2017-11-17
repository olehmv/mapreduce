package co.football.joins;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class MapSideDriver {

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println("Usage: MapSideJoin  <cache> <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "MapSideJoin");
		job.addCacheFile(new Path(args[0]).toUri());
		job.setJarByClass(co.football.joins.MapSideDriver.class);
		job.setMapperClass(MapSideMapper.class);
		job.setReducerClass(MapSideReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		if (!job.waitForCompletion(true))
			return;
	}

	public static class MapSideMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static Map<String, String> map = new HashMap<>();
		private static Pattern p = Pattern.compile("^[A-Z].*$");

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			Path[] files = Job.getInstance(context.getConfiguration()).getLocalCacheFiles();

			FileSystem system = FileSystem.newInstance(context.getConfiguration());
			for (Path f : files) {
				FSDataInputStream open = system.open(f);
				BufferedReader b = new BufferedReader(new InputStreamReader(open));
				String s = null;
				while ((s = b.readLine()) != null) {
					map.put(s.split(",")[0], s.split(",")[1]);
				}
				b.close();
			}

		}

		public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
			String[] strings = ivalue.toString().split(",");
			String string = strings[11];
			Matcher m = p.matcher(string);
			if (m.matches()) {
				if (map.containsKey(string)) {
					context.write(new Text(string), new IntWritable(1));
				}

			}
		}
	}

	public static class MapSideReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text _key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			context.write(_key, new IntWritable(count));
		}
	}
}
