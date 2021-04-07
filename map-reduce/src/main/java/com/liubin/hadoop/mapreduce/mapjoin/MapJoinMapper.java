package com.liubin.hadoop.mapreduce.mapjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * MapJoinMapper.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-07 : base version.
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  private final HashMap<String, String> idAndName = new HashMap<>(16);

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    // 获取缓存的文件
    final URI cacheFile = context.getCacheFiles()[0];
    final BufferedReader bufferedReader =
        new BufferedReader(
            new InputStreamReader(
                new FileInputStream(cacheFile.getPath()), StandardCharsets.UTF_8));
    String line;
    // 读取文件
    while (StringUtils.isNoneEmpty(line = bufferedReader.readLine())) {
      // 03	苹果
      final String[] split = line.split("\t");
      // 缓存到内存
      this.idAndName.put(split[0], split[1]);
    }
    IOUtils.closeStream(bufferedReader);
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 1006	03	6
    final String[] split = value.toString().split("\t");
    final String s =
        String.format("%s\t%s\t%s", split[0], idAndName.getOrDefault(split[1], ""), split[2]);
    context.write(new Text(s), NullWritable.get());
  }
}
