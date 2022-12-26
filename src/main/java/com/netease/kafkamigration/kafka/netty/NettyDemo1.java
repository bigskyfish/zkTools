package com.netease.kafkamigration.kafka.netty;


import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;

public class NettyDemo1 {
    public static void main(String[] args) {
        Channel channel = null;
        // 异步连接到远程节点
        ChannelFuture future = channel.connect(new InetSocketAddress(9999));
        // 注册一个 ChannelFutureListener 到 ChannelFuture
        future.addListener(channelFuture -> {
            if(channelFuture.isSuccess()){
                // 操作成功
                // 构建数据
                Object data = null;
                // 将数据异步发送到远程节点
                ChannelFuture write = channelFuture.getChannel().write(data);

            } else {
                // 操作失败，打赢输出失败原因
                channelFuture.getCause().printStackTrace();
                // 后续 可以根据需求具体处理
            }

        });

    }



}
