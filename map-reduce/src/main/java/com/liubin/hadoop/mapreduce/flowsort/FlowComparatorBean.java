package com.liubin.hadoop.mapreduce.flowsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.StringJoiner;

/**
 * FlowBean.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-01 : base version.
 */
public class FlowComparatorBean implements WritableComparable<FlowComparatorBean> {

  private String id;

  private Long upFlow;

  private Long downFlow;

  private Long totalFlow;

  private Date birthDay;

  public FlowComparatorBean() {}

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(id);
    out.writeLong(upFlow);
    out.writeLong(downFlow);
    out.writeLong(totalFlow);
    out.writeLong(birthDay.getTime());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = in.readUTF();
    this.upFlow = in.readLong();
    this.downFlow = in.readLong();
    this.totalFlow = in.readLong();
    this.birthDay = new Date(in.readLong());
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Long getUpFlow() {
    return upFlow;
  }

  public void setUpFlow(Long upFlow) {
    this.upFlow = upFlow;
  }

  public Long getDownFlow() {
    return downFlow;
  }

  public void setDownFlow(Long downFlow) {
    this.downFlow = downFlow;
  }

  public Long getTotalFlow() {
    return totalFlow;
  }

  public void setTotalFlow(Long totalFlow) {
    this.totalFlow = totalFlow;
  }

  public Date getBirthDay() {
    return birthDay;
  }

  public void setBirthDay(Date birthDay) {
    this.birthDay = birthDay;
  }

  @Override
  public String toString() {
    return new StringJoiner("\t")
        .add("" + id)
        .add("" + upFlow)
        .add("" + downFlow)
        .add("" + totalFlow)
        .add("" + birthDay)
        .toString();
  }

  @Override
  public int compareTo(FlowComparatorBean o) {
    if (o.getTotalFlow() > this.totalFlow) {
      return -1;
    } else if (o.getTotalFlow() < this.totalFlow) {
      return 1;
    }
    return 0;
  }
}
