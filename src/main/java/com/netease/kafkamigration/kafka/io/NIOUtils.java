package com.netease.kafkamigration.kafka.io;

import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.SwitchPoint;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

public class NIOUtils {


    public static void fastCopy(String src, String dist) throws IOException {

        /* 获得源文件的输入字节流 */
        FileInputStream fin = new FileInputStream(src);

        /* 获取输入字节流的文件通道 */
        FileChannel fcin = fin.getChannel();

        /* 获取目标文件的输出字节流 */
        FileOutputStream fout = new FileOutputStream(dist);

        /* 获取输出字节流的通道 */
        FileChannel fcout = fout.getChannel();

        /* 为缓冲区分配 1024 个字节 */
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

        while (true) {

            /* 从输入通道中读取数据到缓冲区中 */
            int r = fcin.read(buffer);

            /* read() 返回 -1 表示 EOF */
            if (r == -1) {
                break;
            }

            /* 切换读写 */
            buffer.flip();

            /* 把缓冲区的内容写入输出文件中 */
            fcout.write(buffer);

            /* 清空缓冲区 */
            buffer.clear();
        }
    }

    private static String readDataFromSocketChannel(SocketChannel sChannel) throws IOException {

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        StringBuilder data = new StringBuilder();

        while (true) {

            buffer.clear();
            int n = sChannel.read(buffer);
            if (n == -1) {
                break;
            }
            buffer.flip();
            int limit = buffer.limit();
            char[] dst = new char[limit];
            for (int i = 0; i < limit; i++) {
                dst[i] = (char) buffer.get(i);
            }
            data.append(dst);
            buffer.clear();
        }
        return data.toString();
    }




    public static void main(String[] args) throws IOException {
        String path = "/Users/netease/operatorWorkSpace/zkTools/src/main/resources/static";
        fastCopy( path + File.separator + "kafka-topic-template-2.5.0.yaml",  path + File.separator + "copy.yaml");

        // 1. 创建选择器
        Selector open = Selector.open();
        // 2. 注册通道
        ServerSocketChannel channel = ServerSocketChannel.open();
        // 2.1 通道设置为非阻塞
        channel.configureBlocking(false);
        // 2.2 注册 事件为OP_ACCEPT
        channel.register(open, SelectionKey.OP_ACCEPT);

        // 3. 构建服务端
        ServerSocket socket = channel.socket();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 8888);
        // 3.1 绑定IP
        socket.bind(address);

        // 4. 事件循环
        while(true){
            // 4.1 获取所有的事件
            open.select();
            // 4.2 获取所有事件 key
            Set<SelectionKey> selectionKeys = open.selectedKeys();
            // 4.3 遍历
            Iterator<SelectionKey> iterator = selectionKeys.iterator();

            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                // 4.4 不同事件处理
                if (key.isAcceptable()) {
                    ServerSocketChannel ssChannel = (ServerSocketChannel) key.channel();
                    // accept 事件创建 channel
                    SocketChannel accept = ssChannel.accept();
                    // channel 非阻塞
                    accept.configureBlocking(false);
                    // 注册 读事件
                    accept.register(open, SelectionKey.OP_READ);

                } else if (key.isReadable()) {
                    SocketChannel socketChannel = (SocketChannel) key.channel();
                    System.out.println(readDataFromSocketChannel(socketChannel));
                    socketChannel.close();
                }
                iterator.remove();
            }

        }


    }



}
