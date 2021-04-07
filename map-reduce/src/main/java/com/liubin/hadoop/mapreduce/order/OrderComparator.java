package com.liubin.hadoop.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.StringJoiner;

/**
 * OrderComparator.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-02 : base version.
 */
public class OrderComparator implements WritableComparable<OrderComparator> {

  private String orderId;

  private BigDecimal price;

  private String goodId;

  public OrderComparator() {}

  public String getOrderId() {
    return orderId;
  }

  public void setOrderId(String orderId) {
    this.orderId = orderId;
  }

  public BigDecimal getPrice() {
    return price;
  }

  public void setPrice(BigDecimal price) {
    this.price = price;
  }

  public String getGoodId() {
    return goodId;
  }

  public void setGoodId(String goodId) {
    this.goodId = goodId;
  }

  @Override
  public int compareTo(OrderComparator o) {
    final int compareTo = new BigDecimal(o.getOrderId()).compareTo(new BigDecimal(this.orderId));
    if (compareTo != 0) {
      return compareTo;
    }
    return -this.price.compareTo(o.getPrice());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.orderId);
    out.writeUTF(this.price.toString());
    out.writeUTF(this.goodId);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.orderId = in.readUTF();
    this.price = new BigDecimal(in.readUTF());
    this.goodId = in.readUTF();
  }

  @Override
  public String toString() {
    return new StringJoiner("\t").add("" + orderId).add("" + price).add("" + goodId).toString();
  }
}
