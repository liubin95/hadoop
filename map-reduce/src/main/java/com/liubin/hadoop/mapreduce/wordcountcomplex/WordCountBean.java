package com.liubin.hadoop.mapreduce.wordcountcomplex;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringJoiner;

/**
 * WordCountBean.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-07 : base version.
 */
public class WordCountBean implements Writable {

  private String fileName;

  private String word;

  private Long num;

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getWord() {
    return word;
  }

  public void setWord(String word) {
    this.word = word;
  }

  public Long getNum() {
    return num;
  }

  public void setNum(Long num) {
    this.num = num;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.fileName);
    out.writeUTF(this.word);
    out.writeLong(this.num);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.fileName = in.readUTF();
    this.word = in.readUTF();
    this.num = in.readLong();
  }

  @Override
  public String toString() {
    return new StringJoiner("\t").add("" + fileName).add("" + word).add("" + num).toString();
  }
}
