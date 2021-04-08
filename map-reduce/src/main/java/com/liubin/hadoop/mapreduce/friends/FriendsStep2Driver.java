package com.liubin.hadoop.mapreduce.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * WordCountDriver.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class FriendsStep2Driver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 获取JOB
    final Configuration configuration = new Configuration();
    final Job job = Job.getInstance(configuration);
    // 存储位置
    job.setJarByClass(FriendsStep2Driver.class);
    // 关联map和reduce
    job.setMapperClass(FriendsStep2Mapper.class);
    job.setReducerClass(FriendsStep2Reduce.class);
    // map 阶段输出的数据类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    // 最终输出类型
    // 为什么不是reduce
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    // 输入路径
    FileInputFormat.setInputPaths(job, new Path("D:/tmp/output/0408/friends-step-1/part-r-00000"));
    FileOutputFormat.setOutputPath(job, new Path("D:/tmp/output/0408/friends-step-2"));
    // 提交job
    final boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }

  private static class FriendsStep2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // G@O	A
      final String[] s = value.toString().split("\t");
      context.write(new Text(s[0]), new Text(s[1]));
    }
  }

  private static class FriendsStep2Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      final List<String> strings =
          StreamSupport.stream(values.spliterator(), false)
              .map(Text::toString)
              .collect(Collectors.toList());
      context.write(key, new Text("\t" + strings.toString()));
    }
  }
}
