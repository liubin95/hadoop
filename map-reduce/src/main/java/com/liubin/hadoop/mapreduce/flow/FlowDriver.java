package com.liubin.hadoop.mapreduce.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * WordCountDriver.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class FlowDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 获取JOB
    final Configuration configuration = new Configuration();
    final Job job = Job.getInstance(configuration);
    // 存储位置
    job.setJarByClass(FlowDriver.class);
    // 关联map和reduce
    job.setMapperClass(FlowMapper.class);
    job.setReducerClass(FlowReducer.class);
    // map 阶段输出的数据类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FlowBean.class);
    // 最终输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlowBean.class);
    // 处理小文件
    job.setInputFormatClass(CombineTextInputFormat.class);
    // 自定义的分区类
    job.setPartitionerClass(FlowPartitioner.class);
    // 设置reduce的数量
    job.setNumReduceTasks(3);
    // 输入路径
    CombineTextInputFormat.setInputPaths(job, new Path("D:/tmp/input/flow.txt"));
    // 设置分区的最大值
    CombineTextInputFormat.setMaxInputSplitSize(job, 10 * 1024 * 1024);
    FileOutputFormat.setOutputPath(job, new Path("D:/tmp/output/0406/flow"));
    // 提交job
    final boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}
