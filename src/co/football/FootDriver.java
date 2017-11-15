package co.football;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class FootDriver {

  public static void main(String[] args) throws Exception {
	 BasicConfigurator.configure();
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Foot-country count");
    job.setJarByClass(co.football.FootDriver.class);
    job.setMapperClass(FootMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setCombinerClass(FootReducer.class);
    job.setPartitionerClass(FootPartitioner.class);
    job.setReducerClass(FootReducer.class);
    job.setNumReduceTasks(2);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    if (!job.waitForCompletion(true))
      return;
  }

  public static class FootPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
      if (key.toString().matches("^[A-K].*$")) {
        return 1 % numPartitions;
      }
      if (key.toString().matches("^[K-Z].*$")) {
        return 2 % numPartitions;
      }
      return 0;
    }

  }

  public static class FootMapper
      extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable ikey, Text ivalue, Context context)
        throws IOException, InterruptedException {
      String[] strings = ivalue.toString().split(",");
      String string = strings[11];
      Pattern p = Pattern.compile("^[A-Z].*$");
      Matcher m = p.matcher(string);
      if (m.matches()) {
        context.write(new Text(string), new IntWritable(1));
      }
    }

  }

  public static class FootReducer
      extends Reducer<Text, IntWritable, Text, IntWritable> {

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
