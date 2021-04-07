package com.liubin.hadoop.mapreduce.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * OrderGroupMapper.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-02 : base version.
 */
public class OrderGroupMapper extends Mapper<LongWritable, Text, OrderComparator, OrderComparator> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 001	pdt_1	222.8
    final String[] strings = value.toString().split("\t");
    final OrderComparator bean = new OrderComparator();
    bean.setOrderId(strings[0]);
    bean.setPrice(new BigDecimal(strings[2]));
    bean.setGoodId(strings[1]);
    context.write(bean, bean);
  }
}
