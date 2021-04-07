package com.liubin.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
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
public class WordCountDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 获取JOB
    final Configuration configuration = new Configuration();
    // 设置使用BZip2压缩，map和reduce之间过程
    configuration.setBoolean("mapreduce.map.output.compress", true);
    configuration.setClass(
        "mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

    // reduce之后压缩设置
    configuration.setBoolean("mapreduce.output.fileoutputformat.compress", true);
    configuration.setClass(
        "mapreduce.output.fileoutputformat.compress.codec",
        BZip2Codec.class,
        CompressionCodec.class);

    final Job job = Job.getInstance(configuration);
    // 存储位置
    job.setJarByClass(WordCountDriver.class);
    // 关联map和reduce
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);
    // map 阶段输出的数据类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    // 最终输出类型
    // 为什么不是reduce
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    // 使用Combiner 优化，使得进入reduce的数据量更少
    //    job.setCombinerClass(WordCountReducer.class);
    // 输入路径
    FileInputFormat.setInputPaths(job, new Path("D:/tmp/input/flow.txt"));
    FileOutputFormat.setOutputPath(job, new Path("D:/tmp/output/0408/word-count"));
    // 提交job
    final boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}
