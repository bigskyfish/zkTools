package com.netease.kafkamigration.util;

import com.netease.kafkamigration.entity.DirCopyWriterResult;
import com.netease.kafkamigration.entity.DirReadResult;
import com.netease.kafkamigration.entity.ZKConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Map;

public class ZKMigrateUtil {

    public static void copy(String fromZKAddress, String toZkAddress, String fromPath, String toPath) {
        CuratorFramework fromZK =
                CuratorFrameworkFactory.newClient(fromZKAddress,new ExponentialBackoffRetry(1000, 3));
        DirReadResult dirReadResult = new DirReadResult(fromZK,fromPath);

        CuratorFramework toZK =
                CuratorFrameworkFactory.newClient(toZkAddress,new ExponentialBackoffRetry(1000, 3));

        DirCopyWriterResult dirWriterResult = new DirCopyWriterResult(toZK,fromPath,toPath,dirReadResult.getAllNodeValue());

        dirWriterResult.write();

        dirReadResult.printResultInfo();

        dirWriterResult.printResultInfo();
    }


    public static void writeMigrateData(ZKConfig zkConfig) {

        CuratorFramework framework = CuratorFrameworkFactory.builder().authorization(zkConfig.getSchema(), zkConfig.getUserAuth().getBytes())
                .connectString(zkConfig.getAddresses())
                .connectionTimeoutMs(3000).retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        DirReadResult dirReadResult = new DirReadResult(framework, zkConfig.getRootPath());
        // TODO 节点数据写入
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
