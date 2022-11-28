package com.netease.kafkamigration.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaTopic {

    private String topicName;
    private int partitions;
    private int factor;
    private Map<String, String> config;

}
