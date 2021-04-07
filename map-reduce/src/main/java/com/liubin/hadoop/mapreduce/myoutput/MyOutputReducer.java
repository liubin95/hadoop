package com.liubin.hadoop.mapreduce.myoutput;

import org.apache.hadoop.io.NullWritable;
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
public class MyOutputReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MyOutputReducer.class);

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    LOGGER.info("WordCountReducer setup");
  }

  @Override
  protected void reduce(Text key, Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {
    values.forEach(
        item -> {
          try {
            context.write(new Text(key.toString() + "\r\n"), NullWritable.get());
          } catch (IOException | InterruptedException e) {
            e.printStackTrace();
          }
        });
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    LOGGER.info("WordCountReducer cleanup");
  }
}
