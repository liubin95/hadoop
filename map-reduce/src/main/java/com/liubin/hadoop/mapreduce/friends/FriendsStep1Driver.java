package com.liubin.hadoop.mapreduce.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class FriendsStep1Driver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 获取JOB
    final Configuration configuration = new Configuration();
    final Job job = Job.getInstance(configuration);
    // 存储位置
    job.setJarByClass(FriendsStep1Driver.class);
    // 关联map和reduce
    job.setMapperClass(FriendsMapper.class);
    job.setReducerClass(FriendsReducer.class);
    // map 阶段输出的数据类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    // 最终输出类型
    // 为什么不是reduce
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    // 输入路径
    FileInputFormat.setInputPaths(job, new Path("D:/tmp/input/friends/friends.txt"));
    FileOutputFormat.setOutputPath(job, new Path("D:/tmp/output/0408/friends-step-1"));
    // 提交job
    final boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}
