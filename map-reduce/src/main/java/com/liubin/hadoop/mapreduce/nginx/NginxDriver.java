package com.liubin.hadoop.mapreduce.nginx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * NginxDriver.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-20 : base version.
 */
public class NginxDriver {

  public static void main(String[] args) throws Exception {

    // 获取JOB
    final Configuration configuration = new Configuration();

    final Job job = Job.getInstance(configuration);
    job.setJarByClass(NginxDriver.class);
    // 关联map和reduce
    job.setMapperClass(NginxMapper.class);
    job.setReducerClass(NginxReducer.class);
    // map 阶段输出的数据类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    // 最终输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(1);
    // 输入路径
    FileInputFormat.setInputPaths(job, new Path("D:\\tmp\\input\\nginx"));
    FileOutputFormat.setOutputPath(job, new Path("D:\\tmp\\output\\0420\\nginx"));
    // 提交job
    final boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}

class NginxMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  public static final String PATTERN_LOG_STRING =
      "^(.*) - (.*) (\\[.*]) \"(.*) (/.*)\" (\\d{3}) (\\d+) \"(.*)\" \"(.*)\" \"(.*)\"";
  public static final String PATTERN_ERROR_STRING =
      "^(.*) \\[error] .* failed (\\(\\d:.*)\\), client: (.*), server: (.*), request: \"([A-Z]*) (.*)\", host: \"(.*)\", referrer: \"(.*)\"";
  public static final Pattern PATTERN_ERROR = Pattern.compile(PATTERN_ERROR_STRING);
  private static final Logger LOGGER = LoggerFactory.getLogger(NginxMapper.class);
  private String fileName;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    // 获取文件名
    final FileSplit fileSplit = (FileSplit) context.getInputSplit();
    // 不同文件设置不同的标识位
    this.fileName = fileSplit.getPath().getName();
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    if (!fileName.contains("error")) {
      final StringJoiner stringJoiner = new StringJoiner("\t");
      Matcher matcher = Pattern.compile(PATTERN_LOG_STRING).matcher(value.toString());
      final boolean ignore = matcher.find();
      for (int i = 1; i < 11; i++) {
        try {
          stringJoiner.add(matcher.group(i));
        } catch (Exception exception) {
          LOGGER.error("解析错误：{}:{}：{}", this.fileName, value.toString(), exception);
        }
      }
      //        final String remoteAddr = matcher.group(1);
      //        final String remoteUser = matcher.group(2);
      //        final String timeLocal = matcher.group(3);
      //        final String method = matcher.group(4);
      //        final String url = matcher.group(5);
      //        final String status = matcher.group(6);
      //        final String bodyBytesSent = matcher.group(7);
      //        final String httpReferer = matcher.group(8);
      //        final String httpUserAgent = matcher.group(9);
      //        final String httpForwardedFor = matcher.group(10);
      context.write(new Text(stringJoiner.toString()), NullWritable.get());
    }
  }
}

class NginxReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

  @Override
  protected void reduce(Text key, Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {
    context.write(key, NullWritable.get());
  }
}
