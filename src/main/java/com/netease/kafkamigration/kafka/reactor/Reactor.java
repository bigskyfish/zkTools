package com.netease.kafkamigration.kafka.reactor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;
import java.util.Set;

public class Reactor implements Runnable {

    private final Selector selector;

    private final ServerSocketChannel socketChannel;

    public Reactor(int port){
        try {
            // 创建服务端的ServerSocketChannel
            socketChannel = ServerSocketChannel.open();
            // 设置为非阻塞模式
            socketChannel.configureBlocking(false);
            // 创建一个Selector多路复用器
            selector = Selector.open();
            // 注册事件
            SelectionKey key = socketChannel.register(selector, SelectionKey.OP_ACCEPT);
            // 绑定服务端端口
            socketChannel.bind(new InetSocketAddress(port));
            // 为服务端Channel绑定一个Acceptor
            key.attach(new Acceptor(socketChannel));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void run() {
        try {
            while(!Thread.interrupted()){
                // 服务端使用一个线程不断等待客户端的连接到达
                selector.select();
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = keys.iterator();
                while(iterator.hasNext()){
                    // 监听到客户端连接事件后将其分发给Acceptor
                    dispatch(iterator.next());
                    iterator.remove();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void dispatch(SelectionKey key){
        // 这里的attachement也即前面为服务端Channel绑定的Acceptor，调用其run()方法进行
        // 客户端连接的获取，并且进行分发
        Runnable attachment = (Runnable) key.attachment();
        attachment.run();
    }
}
