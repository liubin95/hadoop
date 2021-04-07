package com.liubin.hadoop.mapreduce.myoutput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * WordCountMapper.
 *
 * <p>输入数据的key 行号</br> 输入数据的value 字符串</br> 输出数据的key 单词</br> 输出数据的value 数量 1</br>
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class MyOutputMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MyOutputMapper.class);

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    LOGGER.info("WordCountMapper setup");
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 读取一行数据
    context.write(value, NullWritable.get());
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    LOGGER.info("WordCountMapper cleanup");
  }
}
