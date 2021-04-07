package com.liubin.hadoop.mapreduce.myoutput;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MyOutputFormat.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-07 : base version.
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
  @Override
  public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    return new MyRecordWriter(job);
  }
}
