package com.liubin.hadoop.mapreduce.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * OrderGroupReducer.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-02 : base version.
 */
public class OrderGroupReducer
    extends Reducer<OrderComparator, OrderComparator, OrderComparator, NullWritable> {
  @Override
  protected void reduce(OrderComparator key, Iterable<OrderComparator> values, Context context)
      throws IOException, InterruptedException {
    for (int i = 0; i < 2; i++) {
      //
      context.write(values.iterator().next(), NullWritable.get());
    }
  }
}
