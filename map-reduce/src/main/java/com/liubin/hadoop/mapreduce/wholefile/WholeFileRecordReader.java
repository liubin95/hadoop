package com.liubin.hadoop.mapreduce.wholefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * WholeFileRecordReader. 每一个文件执行一次，新对象
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-01 : base version.
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {
  private final Text text = new Text();
  private final BytesWritable bytesWritable = new BytesWritable();
  private FileSplit fileSplit;
  private Configuration configuration;
  private boolean isProgress = true;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    this.fileSplit = (FileSplit) split;
    this.configuration = context.getConfiguration();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (isProgress) {
      // 缓存区
      byte[] bytes = new byte[(int) fileSplit.getLength()];
      // 读取的文件path
      final Path path = fileSplit.getPath();
      // Hadoop 文件系统实例
      final FileSystem fileSystem = path.getFileSystem(configuration);
      // 获取文件的流
      final FSDataInputStream inputStream = fileSystem.open(path);
      // 读取到缓存区
      IOUtils.readFully(inputStream, bytes, 0, (int) fileSplit.getLength());
      // 设置到value
      this.bytesWritable.set(bytes, 0, bytes.length);
      // 设置key
      this.text.set(path.toString());
      // 关闭输入流
      IOUtils.closeStream(inputStream);
      this.isProgress = false;
      return true;
    }
    return false;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return this.text;
  }

  @Override
  public BytesWritable getCurrentValue() throws IOException, InterruptedException {
    return this.bytesWritable;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {}
}
