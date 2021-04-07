package com.liubin.hadoop.mapreduce.wholefile;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * WordCountReducer.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class WholeFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

  @Override
  protected void reduce(Text key, Iterable<BytesWritable> values, Context context)
      throws IOException, InterruptedException {
    context.write(key, values.iterator().next());
  }
}
