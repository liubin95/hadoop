package com.liubin.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * WordCountReducer.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(WordCountReducer.class);

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    LOGGER.info("WordCountReducer setup");
  }

  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    int i = 0;
    for (LongWritable item : values) {
      i += item.get();
    }
    context.write(key, new LongWritable(i));
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    LOGGER.info("WordCountReducer cleanup");
  }
}
