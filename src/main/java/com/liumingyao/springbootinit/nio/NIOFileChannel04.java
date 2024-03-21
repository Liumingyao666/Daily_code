package com.liumingyao.springbootinit.nio;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

public class NIOFileChannel04 {

    /**
     * 使用 FileChannel（通道）和方法 transferFrom，完成文件的拷贝
     * 拷贝图片
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {

        FileInputStream fileInputStream = new FileInputStream("d://1.png");
        FileChannel sourceChannel = fileInputStream.getChannel();

        FileOutputStream fileOutputStream = new FileOutputStream("d://2.png");
        FileChannel destChannel = fileOutputStream.getChannel();

        // 拷贝
        destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());

        sourceChannel.close();
        destChannel.close();
        fileInputStream.close();
        fileOutputStream.close();

    }
}
