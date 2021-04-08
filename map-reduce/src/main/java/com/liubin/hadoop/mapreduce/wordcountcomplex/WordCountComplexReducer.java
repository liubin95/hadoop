package com.liubin.hadoop.mapreduce.wordcountcomplex;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * WordCountReducer.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class WordCountComplexReducer extends Reducer<Text, WordCountBean, Text, NullWritable> {

  @Override
  protected void reduce(Text key, Iterable<WordCountBean> values, Context context)
      throws IOException, InterruptedException {
    List<WordCountBean> wordCountBeans = new ArrayList<>();
    for (WordCountBean item : values) {
      final WordCountBean countBean = new WordCountBean();
      try {
        BeanUtils.copyProperties(countBean, item);
      } catch (IllegalAccessException | InvocationTargetException e) {
        e.printStackTrace();
      }
      wordCountBeans.add(countBean);
    }
    final Map<String, List<WordCountBean>> fileNameAndCount =
        wordCountBeans.stream().collect(Collectors.groupingBy(WordCountBean::getFileName));
    final StringBuilder stringBuilder = new StringBuilder(key.toString()).append("\t");
    fileNameAndCount.forEach(
        (fileName, val) -> {
          final long sum = val.stream().mapToLong(WordCountBean::getNum).sum();
          stringBuilder.append(fileName).append("-->").append(sum).append("\t");
        });
    context.write(new Text(stringBuilder.toString()), NullWritable.get());
  }
}
