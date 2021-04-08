package com.liubin.hadoop.mapreduce.friends;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

/**
 * FriendsWritable.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-08 : base version.
 */
public class FriendsWritable implements WritableComparable<FriendsWritable> {

  private String name;

  private ArrayList<String> friends;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ArrayList<String> getFriends() {
    return friends;
  }

  public void setFriends(ArrayList<String> friends) {
    this.friends = friends;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.name);
    out.writeUTF(this.friends.toString());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.name = in.readUTF();
    final String s = in.readUTF();
    final String[] split = s.substring(1, s.length() - 1).split(",");
    this.friends = new ArrayList<>(Arrays.asList(split));
  }

  @Override
  public String toString() {
    return new StringJoiner("\t").add("" + name).add("" + friends).toString();
  }

  @Override
  public int compareTo(FriendsWritable o) {
    return this.name.compareTo(o.getName());
  }
}
