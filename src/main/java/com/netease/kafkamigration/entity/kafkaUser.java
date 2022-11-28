package com.netease.kafkamigration.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class kafkaUser {

    private String mechanism;
    private String psd;
    private List<KafkaAcls> aclsList;

    // quotas
    private int consumerByteRate;
    private String controllerMutationRate;
    private int producerByteRate;
    private int requestPercentage;

}
