package com.netease.kafkamigration.listen;

import com.netease.kafkamigration.entity.ZKConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

import java.util.List;

public class ZKApplicationListener implements ApplicationListener {


    @Autowired
    private ZKConfig zkConfig;


    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        System.setProperty("zookeeper.authProvider.1","org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        System.setProperty("java.security.auth.login.config","/Users/netease/operatorWorkSpace/zkTools/src/main/resources/zk-jaas.conf");
        System.setProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY,"Client");

        CuratorFramework framework = CuratorFrameworkFactory.builder()
                // .authorization(zkConfig.getSchema(), zkConfig.getUserAuth().getBytes())
                .connectString(zkConfig.getAddresses())
                .connectionTimeoutMs(10000).retryPolicy(new ExponentialBackoffRetry(5000, 3))
                .build();
        CuratorFrameworkState state = framework.getState();
        if(state==CuratorFrameworkState.LATENT) {
            framework.start();
        }


    }
}
