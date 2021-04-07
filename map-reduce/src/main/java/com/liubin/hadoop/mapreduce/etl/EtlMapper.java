package com.liubin.hadoop.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MapJoinMapper.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-07 : base version.
 */
public class EtlMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    final String s = value.toString();
    if (parseLog(s, context)) {
      context.write(value, NullWritable.get());
    }
  }

  private boolean parseLog(String s, Context context) {
    final Counter counterFalse = context.getCounter("map", "false");
    final Counter counterTrue = context.getCounter("map", "true");
    if (s.split(" ").length > 7) {
      counterTrue.increment(1L);
      return true;
    } else {
      counterFalse.increment(1L);
      return false;
    }
  }
}
