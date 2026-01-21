package com.salescode;

import com.salescode.config.AppConfig;
import com.salescode.config.ConfigLoader;
import com.salescode.kafka.KafkaSourceBuilder;
import com.salescode.iceberg.*;
import com.salescode.sink.IcebergSinkBuilder;
import com.salescode.transformer.OrderHeaderTransformer;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.TableLoader;

/**
 * Main Flink Application: Kafka → Transform → Iceberg (S3 + Glue Catalog)
 * 
 * Production-ready CDC pipeline for order data.
 * Uses AWS SDK default credential chain for authentication.
 */
@Slf4j
public class Main {

        public static void main(String[] args) throws Exception {
                log.info("========== Starting Flink CDC Pipeline ==========");

                // 1. Load Configuration
                AppConfig config = ConfigLoader.loadConfig("application.yaml");
                log.info("Configuration loaded: warehouse={}, table={}.{}",
                                config.getIceberg().getWarehouse(),
                                config.getIceberg().getDatabase(),
                                config.getIceberg().getTable());

                // 2. Initialize Flink Environment
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                // 3. Ensure Iceberg table exists
                log.info("Initializing Iceberg table...");
                IcebergTableInitializer.ensureTablesExist(config);
                log.info("Iceberg table ready.");

                // 4. Enable checkpointing (REQUIRED for Iceberg commits)
                // 30 seconds = 30000 milliseconds
                env.enableCheckpointing(30000);
                log.info("Checkpointing enabled: 30 second interval");

                // 5. Build Kafka Source
                KafkaSource<ObjectNode> kafkaSource = KafkaSourceBuilder.build(config.getKafka());
                DataStream<ObjectNode> kafkaStream = env.fromSource(
                                kafkaSource,
                                WatermarkStrategy.noWatermarks(),
                                "KafkaSource");
                log.info("Kafka source initialized.");

                // 6. Transform: Kafka events → Order Headers (55 fields)
                DataStream<ObjectNode> orderHeaderStream = kafkaStream.flatMap(new OrderHeaderTransformer());

                // 7. Sink to Iceberg
                log.info("Setting up Iceberg sink...");
                TableLoader ordersTableLoader = IcebergUtil.ordersTableLoader(config.getIceberg());
                IcebergSinkBuilder.createOrderHeaderSink(orderHeaderStream, ordersTableLoader);
                log.info("Iceberg sink configured: {}.{}",
                                config.getIceberg().getDatabase(),
                                config.getIceberg().getTable());

                // 8. Execute
                log.info("========== Executing Flink Job ==========");
                env.execute("Kafka-to-Iceberg CDC Pipeline");
        }
}
