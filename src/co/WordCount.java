package co;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class WordCount {

  public static void main(String[] args) throws Exception

  {
    // http://blog.cloudera.com/blog/2013/05/how-to-configure-eclipse-for-hadoop-contributions/
    // steps to add log4j.properties file
    // 1 Run
    // 2 Run Configurations
    // 3 Classpath (tab)
    // 4 User Entries
    // 5 Advanced (button on the right)
    // 6 Add Folders
    // 7 then navigate to the folder that contains your log4j.properties file
    // 8 Apply
    // 9 Run

    // set log4j DEBUG level, alternative to log4j.properties file
    BasicConfigurator.configure();
    Configuration c = new Configuration();
    // set jar location
    // c.set("wordcountjar.jar",
    // "/home/hduser/Documents/workspace/mapreduce/src/co/wordcountjar.jar",
    // "co.WordCount");

    String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

    Path input = new Path(files[0]);

    Path output = new Path(files[1]);

    @SuppressWarnings("deprecation")
    Job j = new Job(c, "wordcount");

    j.setJarByClass(WordCount.class);
    // set jar location
    // j.setJar("/home/hduser/Documents/workspace/mapreduce/src/co/wordcount.jar");

    j.setMapperClass(MapForWordCount.class);

    j.setReducerClass(ReduceForWordCount.class);

    j.setOutputKeyClass(Text.class);

    j.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(j, input);

    FileOutputFormat.setOutputPath(j, output);

    System.exit(j.waitForCompletion(true) ? 0 : 1);

  }

  public static class MapForWordCount
      extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context con)
        throws IOException, InterruptedException

    {

      String line = value.toString();

      String[] words = line.split(",");

      for (String word : words)

      {

        Text outputKey = new Text(word.toUpperCase().trim());

        IntWritable outputValue = new IntWritable(1);

        con.write(outputKey, outputValue);

      }

    }

  }

  public static class ReduceForWordCount
      extends Reducer<Text, IntWritable, Text, IntWritable>

  {

    public void reduce(Text word, Iterable<IntWritable> values, Context con)
        throws IOException, InterruptedException

    {

      int sum = 0;

      for (IntWritable value : values)

      {

        sum += value.get();

      }

      con.write(word, new IntWritable(sum));

    }

  }

}