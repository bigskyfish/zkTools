package com.netease.kafkamigration.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaAcls {

    // e.g. topic;group;cluster;transactionalId
    private String resourceType;
    private String resourceName;
    private String patternType;

    // e.g. read;write;create;delete;alter;describe;clusterAction;alterConfigs;describeConfigs;idempotentWrite;all
    private String operation;
    private String host;
    private String type;


}
