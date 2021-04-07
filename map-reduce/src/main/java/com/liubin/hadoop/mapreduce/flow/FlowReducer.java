package com.liubin.hadoop.mapreduce.flow;

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
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlowReducer.class);

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    LOGGER.info("WordCountReducer setup");
  }

  @Override
  protected void reduce(Text key, Iterable<FlowBean> values, Context context)
      throws IOException, InterruptedException {
    final FlowBean bean = new FlowBean();
    bean.setUpFlow(0L);
    bean.setDownFlow(0L);
    bean.setTotalFlow(0L);
    values.forEach(
        item -> {
          bean.setUpFlow(bean.getUpFlow() + item.getUpFlow());
          bean.setDownFlow(bean.getDownFlow() + item.getDownFlow());
          bean.setTotalFlow(bean.getTotalFlow() + item.getTotalFlow());
          bean.setBirthDay(item.getBirthDay());
          bean.setId(item.getId());
        });

    context.write(key, bean);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    LOGGER.info("WordCountReducer cleanup");
  }
}
