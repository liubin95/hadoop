package com.liubin.hadoop.mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * FlowPartitioner. 根据手机号前三位，放置不同的分区
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-02 : base version.
 */
public class FlowPartitioner extends Partitioner<Text, FlowBean> {

  private final HashMap<String, Integer> keyAndPartition = new HashMap<>(16);

  @Override
  public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
    // text手机号
    // flowBean 流量信息
    final String substring = text.toString().substring(0, 3);
    if (keyAndPartition.containsKey(substring)) {
      return keyAndPartition.get(substring);
    } else {
      final int size = keyAndPartition.size();
      keyAndPartition.put(substring, size);
      return size;
    }
  }
}
