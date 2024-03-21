package com.liumingyao.springbootinit.nio;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NIOFileChannel01 {

    private static String str = "nio buffer and channel";
    private static final String filePath = "d:\\NIO_buffer_channel.txt";

    /**
     * 将str写入文件
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // 创建输出流
        FileOutputStream fileOutputStream = new FileOutputStream(filePath);

        // 创建NIO通道
        FileChannel fileChannel = fileOutputStream.getChannel();

        // 创建缓冲区
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        // 写入缓冲区
        byteBuffer.put(str.getBytes());

        // 读写切换
        byteBuffer.flip();

        // 数据写入通道
        fileChannel.write(byteBuffer);
        fileOutputStream.close();
    }




}
