package com.liubin.hadoop.mapreduce.wholefile;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * WholeFileMapper.
 *
 * <p>《文件名，文件》
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class WholeFileMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
  @Override
  protected void map(Text key, BytesWritable value, Context context)
      throws IOException, InterruptedException {
    context.write(key, value);
  }
}
