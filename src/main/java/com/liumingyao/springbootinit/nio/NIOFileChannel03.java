package com.liumingyao.springbootinit.nio;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NIOFileChannel03 {

    private static final String filePath = "d:\\NIO_buffer_channel.txt";
    /**
     * 使用一个 Buffer 完成文件读取、写入
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {

        FileInputStream fileInputStream = new FileInputStream(filePath);
        FileChannel fileInputStreamChannel = fileInputStream.getChannel();

        FileOutputStream fileOutputStream = new FileOutputStream("2.txt");
        FileChannel fileOutputStreamChannel = fileOutputStream.getChannel();


        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);

        while (true) {
            // 循环读1.txt, 清空buffer
            byteBuffer.clear();
            int read = fileInputStreamChannel.read(byteBuffer);
            System.out.println("read = " + read);
            if (read == -1){
                // 表示读完
                break;
            }

            byteBuffer.flip();
            fileOutputStreamChannel.write(byteBuffer);
        }

        fileInputStream.close();
        fileOutputStream.close();

    }
}
