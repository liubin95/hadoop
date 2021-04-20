package com.liubin.hadoop.mapreduce.nginx;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Test.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-20 : base version.
 */
public class Test {

  public static void main(String[] args) {
    // '$remote_addr - $remote_user [$time_local] "$request" '
    //                      '$status $body_bytes_sent "$http_referer" '
    //                      '"$http_user_agent" "$http_x_forwarded_for"';
    String log =
        "172.16.20.35 - - [19/Apr/2021:09:00:12 +0800] \"GET /logo-default.png HTTP/1.1\" 304 0 \"-\" \"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.81 Safari/537.36 SE 2.X MetaSr 1.0\" \"172.16.10.1\"";

    String patternLog =
        "^(.*) - (.*) (\\[.*]) \"(.*) (/.*)\" (\\d{3}) (\\d+) \"(.*)\" \"(.*)\" \"(.*)\"";
    // 创建 Pattern 对象
    Pattern r = Pattern.compile(patternLog);
    // 现在创建 matcher 对象
    Matcher m = r.matcher(log);
    if (m.find()) {
      for (int i = 1; i < 11; i++) {
        //
        System.out.println("Found value: " + m.group(i));
      }
    } else {
      System.err.println("NO MATCH");
    }
  }
}
