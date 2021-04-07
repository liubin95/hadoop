package com.liubin.hadoop.mapreduce.wholefile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * WholeFileInputFormat.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-01 : base version.
 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<Text, BytesWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    return new WholeFileRecordReader();
  }
}
