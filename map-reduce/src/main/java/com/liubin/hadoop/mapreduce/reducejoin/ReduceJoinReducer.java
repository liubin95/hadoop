package com.liubin.hadoop.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * ReduceJoinReducer.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-07 : base version.
 */
public class ReduceJoinReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

  @Override
  protected void reduce(Text key, Iterable<TableBean> values, Context context)
      throws IOException, InterruptedException {
    String s = "";
    final List<TableBean> tableBeans = new ArrayList<>();
    for (TableBean item : values) {
      if (Objects.equals("pd", item.getFlag())) {
        s = item.getpName();
      } else {
        final TableBean bean = new TableBean();
        try {
          BeanUtils.copyProperties(bean, item);
        } catch (IllegalAccessException | InvocationTargetException e) {
          e.printStackTrace();
        }
        tableBeans.add(bean);
      }
    }
    for (TableBean item : tableBeans) {
      item.setpName(s);
      context.write(item, NullWritable.get());
    }
  }
}
