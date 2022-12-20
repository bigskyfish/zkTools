package com.netease.kafkamigration.kafka.reactor;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Acceptor implements Runnable {

    private final ExecutorService executorService = Executors.newFixedThreadPool(50);

    private final ServerSocketChannel socketChannel;

    public Acceptor(ServerSocketChannel socketChannel){
        this.socketChannel = socketChannel;
    }



    @Override
    public void run() {
        try {
            // 获取客户端连接
            SocketChannel accept = socketChannel.accept();
            if(accept != null){
                // 将客户端连接交由线程池处理
                executorService.execute(new Handler(accept));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
