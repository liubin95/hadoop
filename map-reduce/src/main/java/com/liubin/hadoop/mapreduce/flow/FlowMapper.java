package com.liubin.hadoop.mapreduce.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

/**
 * FlowMapper.
 *
 * <p>输入数据的key 行号</br> 输入数据的value 字符串</br> 输出数据的key 单词</br> 输出数据的value 数量 1</br>
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlowMapper.class);

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    LOGGER.info("{} setup", this.getClass().getName());
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 读取一行数据
    final String line = value.toString();
    // 切分单词
    final String[] split = line.split(" ");
    String phone = split[0];
    String upFlow = split[1];
    String downFlow = split[2];
    final FlowBean flowBean = new FlowBean();
    flowBean.setBirthDay(new Date());
    flowBean.setId(UUID.randomUUID().toString());
    flowBean.setUpFlow(Long.parseLong(upFlow));
    flowBean.setDownFlow(Long.parseLong(downFlow));
    flowBean.setTotalFlow(flowBean.getUpFlow() + flowBean.getDownFlow());
    context.write(new Text(phone), flowBean);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    LOGGER.info("{} cleanup", this.getClass().getName());
  }
}
