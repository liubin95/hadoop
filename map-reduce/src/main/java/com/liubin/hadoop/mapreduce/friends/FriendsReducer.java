package com.liubin.hadoop.mapreduce.friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * WordCountReducer.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-03-31 : base version.
 */
public class FriendsReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    // A  [b,c,d,f,g,h,i,k,o]
    // B  [a,e,f,j]
    List<String> valueList =
        StreamSupport.stream(values.spliterator(), false)
            .map(Text::toString)
            .collect(Collectors.toList());
    System.out.println(valueList);
    for (String text : valueList) {
      for (String text1 : valueList) {
        if (Objects.equals(text, text1)) {
          continue;
        }
        context.write(new Text(String.format("%s@%s", text, text1)), key);
      }
    }
  }
}
