package com.liubin.hadoop.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * ReduceJoinMapper.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-07 : base version.
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, TableBean> {

  private String name;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    // 获取文件名
    final FileSplit fileSplit = (FileSplit) context.getInputSplit();
    // 不同文件设置不同的标识位
    this.name = fileSplit.getPath().getName();
   }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 1006	03	6 order
    // 03	苹果 pd
    final String[] split = value.toString().split("\t");
    final TableBean tableBean = new TableBean();

    if (this.name.startsWith("order")) {
      // 订单信息
      tableBean.setId(split[0]);
      tableBean.setPid(split[1]);
      tableBean.setCount(Long.parseLong(split[2]));
      tableBean.setpName("");
      tableBean.setFlag("order");
    } else {
      // pd信息
      tableBean.setId("");
      tableBean.setPid(split[0]);
      tableBean.setCount(0L);
      tableBean.setpName(split[1]);
      tableBean.setFlag("pd");
    }
    context.write(new Text(tableBean.getPid()), tableBean);
  }
}
