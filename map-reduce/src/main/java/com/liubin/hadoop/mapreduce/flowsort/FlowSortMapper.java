package com.liubin.hadoop.mapreduce.flowsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Date;

/**
 * FlowMapper.
 *
 * <p>输入数据的key 行号</br> 输入数据的value 字符串</br> 输出数据的key 单词</br> 输出数据的value 数量 1</br>
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowComparatorBean, Text> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    final String[] strings = value.toString().split("\t");
    final FlowComparatorBean bean = new FlowComparatorBean();
    bean.setId(strings[1]);
    bean.setUpFlow(Long.parseLong(strings[2]));
    bean.setDownFlow(Long.parseLong(strings[3]));
    bean.setTotalFlow(Long.parseLong(strings[4]));
    bean.setBirthDay(new Date());
    context.write(bean, new Text(strings[0]));
  }
}
