package co.sum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class SumDriver {

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    Configuration c = new Configuration();
    // c.set("fs.hdfs.impl",
    // org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    // job.getConfiguration().set("mapreduce.output.basename",
    // "clean_comments.xml");

    String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

    Path input = new Path(files[0]);

    Path output = new Path(files[1]);
    @SuppressWarnings("deprecation")
    Job job = new Job(c, "SumJob");
    // job.setJarByClass(SumDriver.class);
    // set jar location
    job.setJar(
        "/home/hduser/Documents/workspace/mapreduce/src/co/sum/sumdriver.jar");
    job.setMapperClass(SumMapper.class);
    // job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MinMaxCount.class);

    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

  public static class SumMapper
      extends Mapper<LongWritable, Text, Text, MinMaxCount> {
    private Text key = new Text();
    private MinMaxCount value = new MinMaxCount();
    Date cDate = new Date();
    private final static SimpleDateFormat frmt =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public void map(LongWritable ikey, Text ivalue, Context context)
        throws IOException, InterruptedException {
      Map<String, String> map = MRDPUtils.transformXmlToMap(ivalue.toString());
      if (map.isEmpty()) {
        return;
      }
      if (map.containsKey("encoding") || map.containsKey("version")) {
        return;
      }
      String date = map.get("CreationDate");
      if (date == null) {
        return;
      }
      String id = map.get("UserId");
      if (id == null) {
        return;
      }
      try {
        cDate = frmt.parse(date);
      } catch (ParseException e) {
        e.printStackTrace();
      }
      value.setMin(cDate);
      value.setMax(cDate);
      value.setCount(1);
      key.set(id);
      context.write(key, value);

    }

  }

  public static class SumReducer
      extends Reducer<Text, MinMaxCount, Text, MinMaxCount> {
    private MinMaxCount value = new MinMaxCount();

    public void reduce(Text _key, Iterable<MinMaxCount> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (MinMaxCount val : values) {
        if (val == null) {
          continue;
        }
        if (value.getMin().compareTo(val.getMin()) < 0) {
          value.setMin(val.getMin());
        }
        if (value.getMax().compareTo(val.getMax()) > 0) {
          value.setMax(val.getMax());
        }
        sum += val.getCount();
      }
      value.setCount(sum);
      context.write(_key, value);
    }

  }

  public static class MinMaxCount implements Writable {
    private Date min = new Date();
    private Date max = new Date();
    private int count;
    private final static SimpleDateFormat frmt =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public String toString() {
      return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
    }

    public Date getMin() {
      return min;
    }

    public void setMin(Date min) {
      this.min = min;
    }

    public Date getMax() {
      return max;
    }

    public void setMax(Date max) {
      this.max = max;
    }

    public int getCount() {
      return count;
    }

    public void setCount(int count) {
      this.count = count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      min = new Date(in.readLong());
      max = new Date(in.readLong());
      count = new Integer(in.readInt());

    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(min.getTime());
      out.writeLong(max.getTime());
      out.writeInt(count);

    }

  }

  public static class MRDPUtils {

    public static final String[] REDIS_INSTANCES =
        { "p0", "p1", "p2", "p3", "p4", "p6" };

    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
      Map<String, String> map = new HashMap<String, String>();
      try {
        String[] tokens =
            xml.trim().substring(5, xml.trim().length() - 3).split("\"");

        for (int i = 0; i < tokens.length - 1; i += 2) {
          String key = tokens[i].trim();
          String val = tokens[i + 1];

          map.put(key.substring(0, key.length() - 1), val);
        }
      } catch (StringIndexOutOfBoundsException e) {
        System.err.println(xml);
      }

      return map;
    }
  }

}
