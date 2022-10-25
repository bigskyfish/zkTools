package com.netease.kafkamigration.entity;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.data.Stat;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DirCopyWriterResult {


    CuratorFramework curatorFramework;
    private String fromPath;
    private String toPath;
    Map<String,String> valueMap = new ConcurrentHashMap<String, String>();
    static Map<String,String> writeSuccessMap = new ConcurrentHashMap<String, String>();
    static Map<String,String> writeErrorMap = new ConcurrentHashMap<String, String>();

    private AtomicInteger addCnt = new AtomicInteger();
    private AtomicInteger updateCnt = new AtomicInteger();

    public DirCopyWriterResult(CuratorFramework curatorFramework, String fromPath, String toPath, Map<String, String> valueMap) {
        this.curatorFramework = curatorFramework;
        this.fromPath = fromPath;
        this.toPath = toPath;
        this.valueMap = valueMap;
        CuratorFrameworkState state = curatorFramework.getState();
        if(state==CuratorFrameworkState.LATENT) {
            curatorFramework.start();
        }
    }

    public void write() {
        if(valueMap!=null && valueMap.size()>0) {
            for(String path:valueMap.keySet()) {
                String value = valueMap.get(path);
                System.out.println("write fromPath:"+fromPath + " fromPath:"+path+ " ; value "+ value);
                String writePath = path.replaceAll(fromPath,toPath);
                System.out.println("write toPath:"+toPath + " writePath:"+writePath+ " ; value "+value);
                boolean ret = false;
                try {
                    Stat stat = curatorFramework.checkExists().forPath(writePath);
                    if(stat==null) {
                        curatorFramework.checkExists().creatingParentContainersIfNeeded().forPath(writePath);
                        curatorFramework.create().forPath(writePath,value.getBytes());
                        addCnt.incrementAndGet();
                    } else {
                        curatorFramework.setData().forPath(writePath,value.getBytes());
                        updateCnt.incrementAndGet();
                    }
                    ret = true;
                } catch (Exception e) {
                    writeErrorMap.put(path,"writePath:"+writePath+"; value: "+value);
                    e.printStackTrace();
                }
                if(ret) {
                    writeSuccessMap.put(path,writePath);
                }
            }
        }
    }

    public Map<String,String> getWriteErrorMap() {
        return writeErrorMap;
    }

    public int getErrorCnt() {
        int cnt = 0;
        if(writeErrorMap!=null && writeErrorMap.size()>0) {
            cnt = writeErrorMap.size();
        }
        return cnt;
    }

    public int getSuccessCnt() {
        int cnt = 0;
        if(writeSuccessMap!=null && writeSuccessMap.size()>0) {
            cnt = writeSuccessMap.size();
        }
        return cnt;
    }

    public int getAddCnt() {
        return addCnt.get();
    }

    public int getUpdateCnt() {
        return updateCnt.get();
    }

    public void printResultInfo() {
        System.out.println("write node add cnt :" + getAddCnt());
        System.out.println("write node update cnt :" + getUpdateCnt());
        System.out.println("write node error cnt :" + getErrorCnt());
    }

}
