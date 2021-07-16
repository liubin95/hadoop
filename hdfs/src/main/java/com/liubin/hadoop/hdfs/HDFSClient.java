package com.liubin.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * HDFSClient.
 *
 * @author huawei.
 * @version 0.0.1.
 * @serial 2021-03-26 : base version.
 */
public class HDFSClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSClient.class);

    private static FileSystem fileSystem;

    @BeforeAll
    static void beforeAll() throws URISyntaxException, IOException, InterruptedException {
        final Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration, "liubin");
        LOGGER.info("开启链接");
    }

    @AfterAll
    static void afterAll() throws IOException {
        fileSystem.close();
        LOGGER.info("关闭链接");
    }

    /**
     * 文件上传
     *
     * @throws IOException IOException
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        fileSystem.copyFromLocalFile(
                new Path("D:/data/100000_full.json"), new Path("/users/liubin/input/2021-03-29"));
    }

    /**
     * 文件上传 io
     *
     * @throws IOException IOException
     */
    @Test
    public void creat() throws IOException {
        try (FileInputStream fileInputStream =
                     new FileInputStream(new File("D:/data/100000_full.json"));
             final FSDataOutputStream fsDataOutputStream =
                     fileSystem.create(new Path("/users/liubin/input/2021-03-30"))) {
            IOUtils.copyBytes(fileInputStream, fsDataOutputStream, 1024);
        }
    }

    /**
     * 文件下载
     *
     * @throws IOException IOException
     */
    @Test
    void copyToLocalFile() throws IOException {
        fileSystem.copyToLocalFile(
                false, new Path("/users/liubin/output"), new Path("D:/data/output"), true);
    }

    @Test
    void listFiles() throws IOException {
        final RemoteIterator<LocatedFileStatus> listFiles =
                fileSystem.listFiles(new Path("/users"), true);

        while (listFiles.hasNext()) {
            final LocatedFileStatus next = listFiles.next();
            LOGGER.info(
                    "f:{},length:{}kb,permission:{}",
                    next.getPath().getName(),
                    next.getLen() / 1024,
                    next.getPermission());
        }
    }

    @Test
    void listStatus() throws IOException {
        final FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/users"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                LOGGER.info(
                        "f:{},length:{}kb,permission:{}",
                        fileStatus.getPath().getName(),
                        fileStatus.getLen() / 1024,
                        fileStatus.getPermission());
            } else {
                LOGGER.info(
                        "d:{},length:{}kb,permission:{}",
                        fileStatus.getPath().getName(),
                        fileStatus.getLen() / 1024,
                        fileStatus.getPermission());
            }
        }
    }
}
