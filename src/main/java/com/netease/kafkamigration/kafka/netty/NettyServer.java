package com.netease.kafkamigration.kafka.netty;


import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class NettyServer {


    public void bind(int port){
        ServerBootstrap strap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
        // 构造对应的pipeline
        strap.setPipelineFactory(() -> {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast(MessageHandler.class.getName(), new MessageHandler());
            return pipeline;
        });

        // 监听端口
        strap.bind(new InetSocketAddress(port));
    }

    public static void main(String[] args) {
        new NettyServer().bind(9999);
    }


}
