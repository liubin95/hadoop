package com.liubin.hadoop.mapreduce.wordcountcomplex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

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
public class WordCountComplexMapper extends Mapper<LongWritable, Text, Text, WordCountBean> {
  private String name;

  @Override
  protected void setup(Context context) {
    // 获取文件名
    final FileSplit fileSplit = (FileSplit) context.getInputSplit();
    // 不同文件设置不同的标识位
    this.name = fileSplit.getPath().getName();
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 读取一行数据
    final String line = value.toString();
    // 切分单词
    final String[] split = line.split(" ");
    for (String s : split) {
      final WordCountBean countBean = new WordCountBean();
      countBean.setFileName(name);
      countBean.setWord(s);
      countBean.setNum(1L);
      context.write(new Text(s), countBean);
    }
  }
}
