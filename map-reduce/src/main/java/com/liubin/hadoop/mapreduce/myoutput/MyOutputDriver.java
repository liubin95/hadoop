package com.liubin.hadoop.mapreduce.myoutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * WordCountDriver.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class MyOutputDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 获取JOB
    final Configuration configuration = new Configuration();
    final Job job = Job.getInstance(configuration);
    // 存储位置
    job.setJarByClass(MyOutputDriver.class);
    // 关联map和reduce
    job.setMapperClass(MyOutputMapper.class);
    job.setReducerClass(MyOutputReducer.class);
    // map 阶段输出的数据类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    // 最终输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    // 输入路径
    job.setOutputFormatClass(MyOutputFormat.class);
    FileInputFormat.setInputPaths(job, new Path("D:/tmp/input/name.log"));
    FileOutputFormat.setOutputPath(job, new Path("D:/tmp/output/0407/my-output"));
    // 提交job
    final boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}
