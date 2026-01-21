package com.salescode.kafka;

import com.salescode.config.AppConfig;
import com.salescode.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

@Slf4j
public class KafkaSourceBuilder {

    // Prevent instantiation
    private KafkaSourceBuilder() {
    }

    /**
     * Build KafkaSource<ObjectNode> using provided KafkaConfig.
     */
    public static KafkaSource<ObjectNode> build(KafkaConfig config) {

        log.info("Initializing Kafka Source with config:");
        log.info(" - Brokers: {}", config.getBrokers());
        log.info(" - Topic: {}", config.getTopic());
        log.info(" - Group ID: {}", config.getGroupId());
        log.info(" - Read from earliest: {}", config.isReadFromEarliest());

        // Use committedOffsets with EARLIEST fallback:
        // - If committed offsets exist: resume from them (no reprocessing)
        // - If no committed offsets: start from earliest (first run)
        OffsetsInitializer offsets = config.isReadFromEarliest()
                ? OffsetsInitializer.earliest()
                : OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);

        return KafkaSource.<ObjectNode>builder()
                .setBootstrapServers(config.getBrokers())
                .setTopics(config.getTopic())
                .setGroupId(config.getGroupId())
                .setStartingOffsets(offsets)
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(ObjectNode.class))
                .build();
    }

    /**
     * Overload: Build KafkaSource using entire AppConfig.
     */
    public static KafkaSource<ObjectNode> build(AppConfig appConfig) {
        return build(appConfig.getKafka());
    }
}
