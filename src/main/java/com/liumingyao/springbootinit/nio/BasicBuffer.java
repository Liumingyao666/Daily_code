package com.liumingyao.springbootinit.nio;

import java.nio.IntBuffer;

public class BasicBuffer {

    public static void main(String[] args) {
        IntBuffer intBuffer = IntBuffer.allocate(5);

        // 向buffer存放数据
        for (int i = 0; i < intBuffer.capacity(); i++) {
            intBuffer.put(i * 2);
        }

        // 从buffer读取数据
        // 将buffer转换，读写切换
        intBuffer.flip();
        while (intBuffer.hasRemaining()){
            System.out.println(intBuffer.get());
        }
    }
}
