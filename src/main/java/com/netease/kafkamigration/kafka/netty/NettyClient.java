package com.netease.kafkamigration.kafka.netty;



import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.Scanner;

public class NettyClient {

    private final ByteBuffer readHeader  = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    private final ByteBuffer writeHeader = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    private SocketChannel channel;

    private void writeWithHeader(SocketChannel channel, byte[] body) throws IOException {
        writeHeader.clear();
        writeHeader.putInt(body.length);
        writeHeader.flip();
        // channel.write(writeHeader);
        channel.write(ByteBuffer.wrap(body));
    }

    private void read(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int r = channel.read(buffer);
            if (r == -1) {
                throw new IOException("end of stream when reading header");
            }
        }
    }

    public void sendMessage(byte[] body){

        try {
            // 创建客户端通道
            channel = SocketChannel.open();
            channel.socket().setSoTimeout(60000);
            channel.connect(new InetSocketAddress(9999));

            // 客户端发请求
            writeWithHeader(channel, body);

            // 接收服务端相应消息
            readHeader.clear();
            read(channel, readHeader);
            int headerInt = readHeader.getInt(0);
            ByteBuffer bodyBuf = ByteBuffer.allocate(headerInt).order(ByteOrder.BIG_ENDIAN);
            read(channel, bodyBuf);
            System.out.println("<客户端>收到响应内容: " + new String(bodyBuf.array(), "UTF-8") + ",长度:" + headerInt);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    public static void main(String[] args) {
        try {
            NettyClient client = new NettyClient();
            Scanner scan = new Scanner(System.in);
            while (true){
                System.out.println("请输入：");
                String data = scan.next();
                if ("q".equalsIgnoreCase(data)){
                    break;
                }
                client.sendMessage(data.getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
