package com.opensource.nebula.sync;

public class Main {

    public static void main(String[] args) throws Exception {
        String readerJarDirPath = "D:/tmp";
        String writeJarDirPath = "D:/tmp";
        //读取nebula版本
        String readerNebulaVersion = "1.x";
        //写入nebula版本
        String writerNebulaVersion = "3.x";
        //读取meta连接地址
        String readerNebulaMetaAddress = "127.0.0.1:45500";
        //写入graph连接地址
        String writerNebulaGraphAddress = "127.0.0.1:3699";
        //同步哪个space
        String space = "test";
        //初始化同步器
        NebulaDataSync nebulaDataSync = new NebulaDataSync(readerJarDirPath, readerNebulaVersion, readerNebulaMetaAddress, writeJarDirPath, writerNebulaVersion, writerNebulaGraphAddress, space);
        //开始同步
        nebulaDataSync.startSync();
    }

}
