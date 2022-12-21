package com.netease.kafkamigration.kafka.aio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;


public class ServerSocketChannelHandle implements CompletionHandler<AsynchronousSocketChannel, Void> {

    public static final Logger LOGGER = LoggerFactory.getLogger(ServerSocketChannelHandle.class);

    private AsynchronousServerSocketChannel serverSocketChannel;


    public ServerSocketChannelHandle(AsynchronousServerSocketChannel serverSocketChannel) {
        this.serverSocketChannel = serverSocketChannel;
    }


    /**
     * 注意，我们分别观察 this、socketChannel、attachment三个对象的id。
     * 来观察不同客户端连接到达时，这三个对象的变化，以说明ServerSocketChannelHandle的监听模式
     */
    @Override
    public void completed(AsynchronousSocketChannel result, Void attachment) {
        LOGGER.info("completed(AsynchronousSocketChannel result, ByteBuffer attachment)");
        //每次都要重新注册监听(一次注册，一次响应)，但是由于“文件状态标示符”是独享的，所以不需要担心有“漏掉的”事件
        this.serverSocketChannel.accept(attachment, this);

        //为这个新的socketChannel注册“read”事件，以便操作系统在收到数据并准备好后，主动通知应用程序
        //在这里，由于我们要将这个客户端多次传输的数据累加起来一起处理，所以我们将一个stringbuffer对象作为一个“附件”依附在这个channel上
        //
        ByteBuffer readBuffer = ByteBuffer.allocate(50);
        result.read(readBuffer, new StringBuffer(), new SocketChannelReadHandle(result , readBuffer));
    }

    @Override
    public void failed(Throwable exc, Void attachment) {
        LOGGER.info("failed(Throwable exc, ByteBuffer attachment)");
    }
}
