package com.salescode;

import com.salescode.config.AppConfig;
import com.salescode.config.ConfigLoader;
import com.salescode.kafka.KafkaSourceBuilder;
import com.salescode.sink.DynamicLobSink;
import com.salescode.transformer.OrderHeaderTransformer;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Main Flink Application: Kafka → Transform → Iceberg (S3 + Glue Catalog)
 * 
 * Production-ready CDC pipeline for order data with dynamic LOB-based routing.
 * Routes data to ck_orders_{lob} tables based on the 'lob' field in payload.
 * New LOBs are auto-handled via configuration without code changes.
 * 
 * Uses AWS SDK default credential chain for authentication.
 */
@Slf4j
public class Main {

        public static void main(String[] args) throws Exception {
                log.info("========== Starting Flink CDC Pipeline (Dynamic LOB Routing) ==========");

                // 1. Load Configuration
                AppConfig config = ConfigLoader.loadConfig("application.yaml");
                log.info("Configuration loaded: warehouse={}, database={}, known-lobs={}",
                                config.getIceberg().getWarehouse(),
                                config.getIceberg().getDatabase(),
                                config.getIceberg().getKnownLobs());

                // 2. Initialize Flink Environment
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                // 3. Enable checkpointing (REQUIRED for Iceberg commits)
                // 30 seconds = 30000 milliseconds
                env.enableCheckpointing(30000);
                log.info("Checkpointing enabled: 30 second interval");

                // 4. Build Kafka Source
                KafkaSource<ObjectNode> kafkaSource = KafkaSourceBuilder.build(config.getKafka());
                DataStream<ObjectNode> kafkaStream = env.fromSource(
                                kafkaSource,
                                WatermarkStrategy.noWatermarks(),
                                "KafkaSource");
                log.info("Kafka source initialized.");

                // 5. Transform: Kafka events → Order Headers (55 fields)
                DataStream<ObjectNode> orderHeaderStream = kafkaStream.flatMap(new OrderHeaderTransformer());

                // 6. Dynamic LOB Sink: Routes to ck_orders_{lob} tables
                log.info("Setting up dynamic LOB-based Iceberg sinks...");
                DynamicLobSink.create(orderHeaderStream, config.getIceberg(), env);
                log.info("Dynamic LOB sinks configured for LOBs: {}", config.getIceberg().getKnownLobs());

                // 7. Execute
                log.info("========== Executing Flink Job ==========");
                env.execute("Kafka-to-Iceberg CDC Pipeline (Multi-LOB)");
        }
}
