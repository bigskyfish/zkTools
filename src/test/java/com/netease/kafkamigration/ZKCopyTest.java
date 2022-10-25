package com.netease.kafkamigration;


import com.netease.kafkamigration.entity.ZKConfig;
import com.netease.kafkamigration.util.ZKMigrateUtil;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.apache.zookeeper.server.auth.IPAuthenticationProvider;
import org.apache.zookeeper.server.auth.SASLAuthenticationProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ZKCopyTest {

    @Autowired
    ZKConfig zkConfig;

    @Test
    public  void test() throws IOException {
        // ZKMigrateUtil.copy("127.0.0.1:2182", "127.0.0.1:2181", "/kafka-1019", "/kafka-1019");
        System.out.println(new DigestAuthenticationProvider().getScheme());
        System.out.println(new SASLAuthenticationProvider().getScheme());
        System.out.println(new IPAuthenticationProvider().getScheme());
        ZKMigrateUtil.writeMigrateData(zkConfig);
    }

}
