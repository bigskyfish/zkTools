package com.netease.kafkamigration.entity;

import lombok.Data;
import lombok.ToString;
import org.apache.curator.framework.AuthInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;


@Data
@ToString
@Component
@PropertySource(value = {"classpath:zk-config.properties"})
public class ZKConfig {

    @Value("${addresses}")
    private String addresses;

    @Value("${root_path}")
    private String rootPath;

    @Value("${schema}")
    private String schema;

    @Value("${user_auth}")
    private String userAuth;

    private List<AuthInfo> adminAuth;

    public ZKConfig(){
        List<AuthInfo> authInfos = new ArrayList<AuthInfo>();
        //user_auth=user1:password1,user2:password2
        if (!StringUtils.isEmpty(this.userAuth)) {
            String[] authList = this.userAuth.split(",");
            for (String singleAuthInfo : authList) {
                authInfos.add(new AuthInfo(this.schema, singleAuthInfo.getBytes()));
            }
        }
        this.adminAuth = authInfos;
    }

}
