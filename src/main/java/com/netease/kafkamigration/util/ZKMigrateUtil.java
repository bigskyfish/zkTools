package com.netease.kafkamigration.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netease.kafkamigration.entity.DirCopyWriterResult;
import com.netease.kafkamigration.entity.DirReadResult;
import com.netease.kafkamigration.entity.ZKConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ZKMigrateUtil {


    public static final Logger LOGGER = LoggerFactory.getLogger(ZKMigrateUtil.class);


    public static Map<String,String> getAllChildren(CuratorFramework curatorFramework, String rootPath){
        CuratorFrameworkState state = curatorFramework.getState();
        if (state == CuratorFrameworkState.LATENT) {
            curatorFramework.start();
        }
        return getAllChildren(curatorFramework, rootPath, rootPath, 0, new ConcurrentHashMap<>());
    }


    private static Map<String,String> getAllChildren(CuratorFramework curatorFramework, String parentPath, String rootPath, int level, Map<String,String> valueMap) {
        try {
            level++;
            Stat stat = curatorFramework.checkExists().forPath(parentPath);
            //  {{rootPath}}/brokers/ids 剔除
            if(stat!=null && !(rootPath + "/brokers/ids").equals(parentPath)) {
                List<String> children = curatorFramework.getChildren().forPath(parentPath);
                if (children != null && children.size()>0) {
                    for(String path : children) {
                        String fullPath = parentPath + "/" + path;
                        stat = curatorFramework.checkExists().forPath(fullPath);
                        if(stat==null) {
                            System.out.print("no data:" + " parentpath "+ parentPath+ "; fullpath:"+fullPath+"  ");
                        } else if (!(rootPath + "/controller").equals(fullPath)) {
                            // {{rootPath}}/controller  剔除
                            byte[] byteValue = curatorFramework.getData().forPath(fullPath);
                            String value = byteValue != null ? new String(byteValue) : "null";
                            valueMap.put(fullPath,value);
                            getAllChildren(curatorFramework, fullPath, rootPath, level, valueMap);
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("getAllChildren is error!", e);
            curatorFramework.close();
        }
        return valueMap;
    }


    public void writeDataToZK(CuratorFramework curatorFramework, Map<String,String> valueMap, String fromPath, String toPath ) {
        CuratorFrameworkState state = curatorFramework.getState();
        if (state == CuratorFrameworkState.LATENT) {
            curatorFramework.start();
        }
        if(valueMap != null && valueMap.size() > 0) {
            for(String path : valueMap.keySet()) {
                String value = valueMap.get(path);
                String writePath = path.replaceAll(fromPath,toPath);
                LOGGER.info("write fromPath:{}; toPath:{}; value: {}", path, toPath, value);
                try {
                    Stat stat = curatorFramework.checkExists().forPath(writePath);
                    if(stat == null) {
                        curatorFramework.checkExists().creatingParentContainersIfNeeded().forPath(writePath);
                        curatorFramework.create().forPath(writePath,value.getBytes());
                    } else {
                        curatorFramework.setData().forPath(writePath,value.getBytes());
                    }
                } catch (Exception e) {
                    LOGGER.error("writeDataToZK is error!", e);
                }
            }
        }
    }


    public static void writeMigrateData(ZKConfig zkConfig) {
        System.setProperty("zookeeper.authProvider.1","org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        System.setProperty("java.security.auth.login.config","/Users/netease/operatorWorkSpace/zkTools/src/main/resources/zk-jaas.conf");
        System.setProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY,"Client");
        CuratorFramework framework = CuratorFrameworkFactory.builder()
                .connectString(zkConfig.getAddresses())
                .connectionTimeoutMs(10000).retryPolicy(new ExponentialBackoffRetry(5000, 3))
                .build();
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String jsonStr = objectMapper.writeValueAsString(getAllChildren(framework, zkConfig.getRootPath()));

        } catch (JsonProcessingException e) {

        }


    }





    public static void printMapInfo(Map<String, Map<String,String>> childrenMap) {
        if(childrenMap!=null && childrenMap.size()>0) {
            for(String parent:childrenMap.keySet()) {
                System.out.println("******************************************");
                System.out.println(parent);
            }
        }
    }

}
