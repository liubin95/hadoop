package com.liubin.hadoop.mapreduce.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * WordCountDriver.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class MapJoinDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
    // 获取JOB
    final Configuration configuration = new Configuration();
    final Job job = Job.getInstance(configuration);
    // 存储位置
    job.setJarByClass(MapJoinDriver.class);
    // 关联map和reduce
    job.setMapperClass(MapJoinMapper.class);
    // 最终输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    // 本例中不需要reduce处理数据，所以省去
    job.setNumReduceTasks(0);
    job.setCacheFiles(new URI[] {new URI("file:///D:/tmp/input/join/pd.txt")});
    // 输入路径
    FileInputFormat.setInputPaths(job, new Path("D:/tmp/input/join/order.txt"));
    FileOutputFormat.setOutputPath(job, new Path("D:/tmp/output/0407/map-join"));
    // 提交job
    final boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}
