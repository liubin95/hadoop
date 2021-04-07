package com.liubin.hadoop.mapreduce.wholefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * WordCountDriver.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class WholeFileDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 获取JOB
    final Configuration configuration = new Configuration();
    final Job job = Job.getInstance(configuration);
    // 存储位置
    job.setJarByClass(WholeFileDriver.class);
    // 关联map和reduce
    job.setMapperClass(WholeFileMapper.class);
    job.setReducerClass(WholeFileReducer.class);
    // map 阶段输出的数据类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(BytesWritable.class);
    // 最终输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    // 输入路径
    job.setInputFormatClass(WholeFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, new Path("D:/tmp"));
    FileOutputFormat.setOutputPath(job, new Path("D:/data/output/0401/whole-file"));
    // 提交job
    final boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}
