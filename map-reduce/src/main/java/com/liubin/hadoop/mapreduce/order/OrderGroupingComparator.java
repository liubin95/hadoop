package com.liubin.hadoop.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.util.Objects;

/**
 * OrderComparator.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-02 : base version.
 */
public class OrderGroupingComparator extends WritableComparator {

  public OrderGroupingComparator() {
    super(OrderComparator.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    // id相同，是相同的Key
    final OrderComparator a1 = (OrderComparator) a;
    final OrderComparator b1 = (OrderComparator) b;
    if (Objects.equals(a1.getOrderId(), b1.getOrderId())) {
      return 0;
    } else {
      return 1;
    }
  }
}
