package com.netease.kafkamigration.entity;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DirReadResult {

    private String rootPath;
    CuratorFramework curatorFramework;
    Map<String, Map<String,String>> relationMap = new ConcurrentHashMap<String, Map<String, String>>();
    Map<String,String> valueMap = new ConcurrentHashMap<String, String>();
    Map<String,String> ephemeralMap = new ConcurrentHashMap<String, String>();
    Map<String,String> emptyValueMap = new ConcurrentHashMap<String, String>();

    public DirReadResult(CuratorFramework curatorFramework, String rootPath) {
        this.rootPath = rootPath;
        this.curatorFramework = curatorFramework;
        CuratorFrameworkState state = curatorFramework.getState();
        if(state==CuratorFrameworkState.LATENT) {
            curatorFramework.start();
        }
        getAllChildren(rootPath,0);
    }

    public String getRootPath() {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    public Map<String,Map<String,String>> getRelationMap() {
        return relationMap;
    }

    public Map<String,String> getAllNodeValue() {
        return valueMap;
    }

    public Map<String,String> getEmptyValueMap() {
        return emptyValueMap;
    }

    public int getErrorCnt() {
        int cnt = 0;
        if(emptyValueMap !=null && emptyValueMap.size()>0) {
            cnt = emptyValueMap.size();
        }
        return cnt;
    }

    public int getEphemeralCnt() {
        int cnt = 0;
        if(ephemeralMap!=null && ephemeralMap.size()>0) {
            cnt = ephemeralMap.size();
        }
        return cnt;
    }

    public int getTotalCnt() {
        int cnt = 0;
        if(valueMap!=null && valueMap.size()>0) {
            cnt = valueMap.size();
        }
        return cnt;
    }

    private void getAllChildren(String parentPath, int level) {
        try {
            level++;
            Stat stat = curatorFramework.checkExists().forPath(parentPath);
            //  {{rootPath}}/brokers/ids 剔除
            if(stat!=null && !(this.rootPath + "/brokers/ids").equals(parentPath)) {
                List<String> children = curatorFramework.getChildren().forPath(parentPath);
                if(children!=null&&children.size()>0) {
                    Map<String,String> childMap = new ConcurrentHashMap<String, String>();
                    for(String path:children) {
                        String fullPath = parentPath+"/"+path;
                        stat = curatorFramework.checkExists().forPath(fullPath);
                        if(stat==null) {
                            System.out.print("no data:" + " parentpath "+ parentPath+ "; fullpath:"+fullPath+"  ");
                            continue;
                        } else if (!(this.rootPath + "/controller").equals(fullPath)) {
                            // {{rootPath}}/controller  剔除
                            byte[] byteValue = curatorFramework.getData().forPath(fullPath);
                            String value = byteValue!=null?new String(byteValue):"null";
                            boolean isEphemeral = stat.getEphemeralOwner()!=0?true:false;
                            if(isEphemeral) {
                                ephemeralMap.put(fullPath,value);
                            }
                            if("null".equals(value)) {
                                emptyValueMap.put(fullPath,"null");
                            }
                            childMap.put(fullPath,value);
                            valueMap.put(fullPath,value);
                            getAllChildren(fullPath,level);
                        }
                    }
                    byte[] parentByteValue = curatorFramework.getData().forPath(parentPath);
                    if(parentByteValue!=null) {
                        valueMap.put(parentPath,new String(parentByteValue));
                    } else {
                        emptyValueMap.put(parentPath,"null");
                    }
                    relationMap.put(parentPath,childMap);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void printResultInfo() {
        System.out.println("total cnt:" + getTotalCnt());
        System.out.println("ephemeral cnt:" + getEphemeralCnt());
        System.out.println("error cnt:" + getErrorCnt());
    }

}
