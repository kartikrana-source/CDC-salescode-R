package com.salescode.config;

import lombok.Data;

@Data
public class KafkaConfig {
    private String brokers;
    private String topic;
    private String groupId;
    private boolean readFromEarliest;
}
