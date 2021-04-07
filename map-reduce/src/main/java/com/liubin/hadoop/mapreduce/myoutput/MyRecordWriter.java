package com.liubin.hadoop.mapreduce.myoutput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * MyRecordWriter.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-07 : base version.
 */
public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

  private FSDataOutputStream fsDataOutputStream;
  private FSDataOutputStream otherOutputStream;

  public MyRecordWriter(TaskAttemptContext job) {
    // 获取文件系统
    try {
      final FileSystem fileSystem = FileSystem.get(job.getConfiguration());
      fsDataOutputStream = fileSystem.create(new Path("d:/tmp/output/0407/liubin.log"));
      otherOutputStream = fileSystem.create(new Path("d:/tmp/output/0407/other.log"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void write(Text key, NullWritable value) throws IOException, InterruptedException {
    if (key.toString().contains("liubin")) {
      fsDataOutputStream.write(key.toString().getBytes());
    } else {
      otherOutputStream.write(key.toString().getBytes());
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    IOUtils.closeStream(fsDataOutputStream);
    IOUtils.closeStream(otherOutputStream);
  }
}
