package com.liubin.hadoop.mapreduce.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
public class OrderGroupDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 获取JOB
    final Configuration configuration = new Configuration();
    final Job job = Job.getInstance(configuration);
    // 存储位置
    job.setJarByClass(OrderGroupDriver.class);
    // 关联map和reduce
    job.setMapperClass(OrderGroupMapper.class);
    job.setReducerClass(OrderGroupReducer.class);
    // map 阶段输出的数据类型
    job.setMapOutputKeyClass(OrderComparator.class);
    job.setMapOutputValueClass(OrderComparator.class);
    // 最终输出类型
    job.setOutputKeyClass(OrderComparator.class);
    job.setOutputValueClass(NullWritable.class);
    // 分组，将
    job.setGroupingComparatorClass(OrderGroupingComparator.class);
    // 输入路径
    FileInputFormat.setInputPaths(job, new Path("D:/tmp/input/order"));
    FileOutputFormat.setOutputPath(job, new Path("D:/tmp/output/0406/order-group"));
    // 提交job
    final boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}
