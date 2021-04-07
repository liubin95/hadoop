package com.liubin.hadoop.mapreduce.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * 测试压缩和解压缩
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-07 : base version.
 */
public class Main {

  public static void main(String[] args) throws Exception {
    //    compression("org.apache.hadoop.io.compress.BZip2Codec");
    //    compression("org.apache.hadoop.io.compress.GzipCodec");
    //    compression("org.apache.hadoop.io.compress.DefaultCodec");
    deCompression("D:/tmp/input/logs.bz2");
    deCompression("D:/tmp/input/logs.deflate");
    deCompression("D:/tmp/input/logs.gz");
  }

  public static void compression(String className) throws Exception {
    final String fileName = "D:/tmp/input/logs";
    // 输入流
    FileInputStream fileInputStream = new FileInputStream(fileName);
    // 压缩类
    final CompressionCodec compressionCodec =
        (CompressionCodec)
            ReflectionUtils.newInstance(Class.forName(className), new Configuration());
    // 输出流
    FileOutputStream fileOutputStream =
        new FileOutputStream(fileName + compressionCodec.getDefaultExtension());
    // 压缩输出流
    final CompressionOutputStream outputStream =
        compressionCodec.createOutputStream(fileOutputStream);
    // 流的复制
    IOUtils.copyBytes(fileInputStream, outputStream, 1024 * 1024 * 5, false);
    // 关闭文件
    IOUtils.closeStream(outputStream);
    IOUtils.closeStream(fileOutputStream);
    IOUtils.closeStream(fileInputStream);
  }

  private static void deCompression(String fileName) throws Exception {
    final CompressionCodecFactory codecFactory = new CompressionCodecFactory(new Configuration());
    final CompressionCodec codec = codecFactory.getCodec(new Path(fileName));
    if (codec == null) {
      throw new RuntimeException("deCompression fail");
    }
    final FileInputStream inputStream = new FileInputStream(new File(fileName));
    final CompressionInputStream compressionInputStream = codec.createInputStream(inputStream);
    final FileOutputStream outputStream = new FileOutputStream(new File(fileName + ".decode"));
    IOUtils.copyBytes(compressionInputStream, outputStream, 1024 * 1024 * 5, false);
    IOUtils.closeStream(outputStream);
    IOUtils.closeStream(compressionInputStream);
    IOUtils.closeStream(inputStream);
  }
}
