package com.liubin.hadoop.mapreduce.flowsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * WordCountReducer.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class FlowSortReducer extends Reducer<FlowComparatorBean, Text, Text, FlowComparatorBean> {

  @Override
  protected void reduce(FlowComparatorBean key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    context.write(values.iterator().next(), key);
  }
}
