package com.liubin.hadoop.mapreduce.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringJoiner;

/**
 * TableBean.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-07 : base version.
 */
public class TableBean implements Writable {

  private String id;

  private String pid;

  private Long count;

  private String pName;

  private String flag;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getPid() {
    return pid;
  }

  public void setPid(String pid) {
    this.pid = pid;
  }

  public Long getCount() {
    return count;
  }

  public void setCount(Long count) {
    this.count = count;
  }

  public String getpName() {
    return pName;
  }

  public void setpName(String pName) {
    this.pName = pName;
  }

  public String getFlag() {
    return flag;
  }

  public void setFlag(String flag) {
    this.flag = flag;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.id);
    out.writeUTF(this.pid);
    out.writeLong(this.count);
    out.writeUTF(this.pName);
    out.writeUTF(this.flag);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = in.readUTF();
    this.pid = in.readUTF();
    this.count = in.readLong();
    this.pName = in.readUTF();
    this.flag = in.readUTF();
  }

  @Override
  public String toString() {
    return new StringJoiner("\t")
        .add("" + id)
        .add("" + count)
        .add("" + pName)
        .add("" + flag)
        .toString();
  }
}
