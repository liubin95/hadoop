package com.liubin.hadoop.mapreduce.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
public class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 读取一行数据
    final String line = value.toString();
    // 切分单词
    // A:B,C,D,E,F,O
    final String[] split = line.split(":");
    final String[] friendArr = split[1].split(",");
    for (String s : friendArr) {
      context.write(new Text(s), new Text(split[0]));
      System.out.println(s + "------" + split[0]);
    }
  }
}
