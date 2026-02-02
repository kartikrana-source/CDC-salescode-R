package com.salescode.sink;

import com.salescode.config.IcebergConfig;
import com.salescode.iceberg.CreateIcebergTables;
import com.salescode.iceberg.IcebergUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dynamic LOB-based sink that routes data to Iceberg tables based on the 'lob'
 * field.
 * Tables are created dynamically as ck_orders_{lob} when a new LOB is
 * encountered.
 * 
 * Usage:
 * DynamicLobSink.create(orderHeaderStream, config, env);
 */
@Slf4j
public class DynamicLobSink implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String TABLE_PREFIX = "ck_orders_";
    private static final DateTimeFormatter CUSTOM_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter WITH_MILLIS_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private DynamicLobSink() {
        // Use static factory method
    }

    /**
     * Create dynamic LOB-based sinks for the given stream.
     * This method discovers unique LOBs in the stream and creates separate sinks
     * for each.
     * 
     * @param stream        The input stream with ObjectNode records containing
     *                      'lob' field
     * @param icebergConfig Iceberg configuration
     * @param env           Flink execution environment
     * @param knownLobs     Set of known LOBs to pre-create sinks for
     */
    public static void create(
            DataStream<ObjectNode> stream,
            IcebergConfig icebergConfig,
            StreamExecutionEnvironment env,
            Set<String> knownLobs) {

        log.info("Creating dynamic LOB sinks for known LOBs: {}", knownLobs);

        // Ensure tables exist for all known LOBs
        ensureTablesExist(icebergConfig, knownLobs);

        // Create output tags for each LOB
        Map<String, OutputTag<ObjectNode>> lobTags = new ConcurrentHashMap<>();
        for (String lob : knownLobs) {
            lobTags.put(lob.toLowerCase(), new OutputTag<ObjectNode>(lob.toLowerCase()) {
            });
        }

        // Default output tag for unknown LOBs
        OutputTag<ObjectNode> unknownLobTag = new OutputTag<ObjectNode>("unknown_lob") {
        };

        // Route records by LOB using side outputs
        var routedStream = stream.process(new LobRouterFunction(lobTags, unknownLobTag));

        // Create sink for each known LOB
        for (String lob : knownLobs) {
            String lobLower = lob.toLowerCase();
            String tableName = TABLE_PREFIX + lobLower;

            DataStream<ObjectNode> lobStream = routedStream.getSideOutput(lobTags.get(lobLower));
            DataStream<RowData> rowStream = lobStream.map(new OrderHeaderMapper());

            TableLoader tableLoader = IcebergUtil.tableLoader(icebergConfig, tableName);

            // UPSERT mode: Updates existing records with same id + creation_time
            // Requires creation_time to NEVER be null and NEVER change for same order
            FlinkSink.forRowData(rowStream)
                    .tableLoader(tableLoader)
                    .equalityFieldColumns(java.util.List.of("id", "creation_time"))
                    .upsert(true)
                    .writeParallelism(1)
                    .append();

            log.info("✔ Created sink for LOB '{}' → table '{}'", lob, tableName);
        }

        // Handle unknown LOBs - log them for now
        routedStream.getSideOutput(unknownLobTag)
                .process(new ProcessFunction<ObjectNode, ObjectNode>() {
                    @Override
                    public void processElement(ObjectNode value, Context ctx, Collector<ObjectNode> out) {
                        String lob = value.has("lob") ? value.get("lob").asText() : "null";
                        log.warn("⚠ Unknown LOB encountered: '{}'. Record will be skipped. " +
                                "Add this LOB to the configuration to process it.", lob);
                    }
                });
    }

    /**
     * Create dynamic LOB sinks by extracting LOBs from configuration.
     * Auto-discovers LOBs from the config file.
     */
    public static void create(
            DataStream<ObjectNode> stream,
            IcebergConfig icebergConfig,
            StreamExecutionEnvironment env) {

        // Extract known LOBs from config (if configured) or use empty set
        Set<String> knownLobs = icebergConfig.getKnownLobs();
        if (knownLobs == null || knownLobs.isEmpty()) {
            log.warn("No known LOBs configured. All LOBs will be logged as unknown. " +
                    "Configure 'iceberg.known-lobs' in application.yaml.");
            knownLobs = Set.of();
        }

        create(stream, icebergConfig, env, knownLobs);
    }

    /**
     * Ensure Iceberg tables exist for all known LOBs.
     */
    private static void ensureTablesExist(IcebergConfig icebergConfig, Set<String> lobs) {
        try {
            CatalogLoader catalogLoader = IcebergUtil.glueCatalogLoader(icebergConfig);
            Catalog catalog = catalogLoader.loadCatalog();

            for (String lob : lobs) {
                String tableName = TABLE_PREFIX + lob.toLowerCase();
                CreateIcebergTables.createOrdersTableWithName(catalog, icebergConfig, tableName);
            }

            log.info("✔ Ensured {} LOB tables exist", lobs.size());
        } catch (Exception e) {
            log.error("Failed to ensure LOB tables exist: {}", e.getMessage(), e);
            throw new RuntimeException("LOB table initialization failed", e);
        }
    }

    // ============================================================
    // LOB Router Function - routes records to side outputs by LOB
    // ============================================================

    private static class LobRouterFunction extends ProcessFunction<ObjectNode, ObjectNode> {

        private final Map<String, OutputTag<ObjectNode>> lobTags;
        private final OutputTag<ObjectNode> unknownLobTag;

        public LobRouterFunction(Map<String, OutputTag<ObjectNode>> lobTags,
                OutputTag<ObjectNode> unknownLobTag) {
            this.lobTags = lobTags;
            this.unknownLobTag = unknownLobTag;
        }

        @Override
        public void processElement(ObjectNode value, Context ctx, Collector<ObjectNode> out) {
            String lob = value.has("lob") ? value.get("lob").asText() : null;

            if (lob == null || lob.isEmpty()) {
                ctx.output(unknownLobTag, value);
                return;
            }

            String lobLower = lob.toLowerCase();
            OutputTag<ObjectNode> tag = lobTags.get(lobLower);

            if (tag != null) {
                ctx.output(tag, value);
            } else {
                ctx.output(unknownLobTag, value);
            }
        }
    }

    // ============================================================
    // Order Header Mapper - same as IcebergSinkBuilder
    // ============================================================

    private static class OrderHeaderMapper implements MapFunction<ObjectNode, RowData> {

        @Override
        public RowData map(ObjectNode node) throws Exception {
            GenericRowData row = new GenericRowData(56);

            // 0. id (required)
            row.setField(0, getString(node, "id"));
            if (row.getField(0) == "26879eb2b-1j") {
                System.out.println("here");
            }

            // 1-2. Status Fields
            row.setField(1, getString(node, "active_status"));
            row.setField(2, getString(node, "active_status_reason"));

            // 3-7. Audit Fields
            row.setField(3, getString(node, "created_by"));
            row.setField(4, getTimestamp(node, "creation_time"));
            row.setField(5, getTimestamp(node, "last_modified_time"));
            row.setField(6, getString(node, "modified_by"));
            row.setField(7, getTimestamp(node, "system_time"));

            // 8-10. Business Fields
            row.setField(8, getString(node, "lob"));
            row.setField(9, getInt(node, "version"));
            row.setField(10, getString(node, "source"));

            // 11-21. Amount and Quantity Fields
            row.setField(11, getDouble(node, "bill_amount"));
            row.setField(12, getDouble(node, "net_amount"));
            row.setField(13, getDouble(node, "total_amount"));
            row.setField(14, getDouble(node, "total_initial_amt"));
            row.setField(15, getFloat(node, "total_initial_quantity"));
            row.setField(16, getDouble(node, "total_mrp"));
            row.setField(17, getFloat(node, "total_quantity"));
            row.setField(18, getFloat(node, "normalized_quantity"));
            row.setField(19, getFloat(node, "initial_normalized_quantity"));
            row.setField(20, getFloat(node, "normalized_volume"));
            row.setField(21, getInt(node, "line_count"));

            // 22-26. Order Identifiers
            row.setField(22, getString(node, "order_number"));
            row.setField(23, getString(node, "reference_number"));
            row.setField(24, getString(node, "reference_order_number"));
            row.setField(25, getString(node, "remarks"));
            row.setField(26, getString(node, "ship_id"));

            // 27-31. Location & Hierarchy
            row.setField(27, getString(node, "location_hierarchy"));
            row.setField(28, getString(node, "outletcode"));
            row.setField(29, getString(node, "supplierid"));
            row.setField(30, getString(node, "hierarchy"));
            row.setField(31, getString(node, "user_hierarchy"));

            // 32-33. GPS
            row.setField(32, getString(node, "gps_latitude"));
            row.setField(33, getString(node, "gps_longitude"));

            // 34-39. Type & Status
            row.setField(34, getString(node, "type"));
            row.setField(35, getString(node, "sub_type"));
            row.setField(36, getString(node, "status"));
            row.setField(37, getString(node, "status_reason"));
            row.setField(38, getString(node, "processing_status"));
            row.setField(39, getString(node, "channel"));

            // 40-41. Dates
            row.setField(40, getTimestamp(node, "delivery_date"));
            row.setField(41, getTimestamp(node, "sales_date"));

            // 42-45. Beat Info
            row.setField(42, getString(node, "beat"));
            row.setField(43, getString(node, "beat_name"));
            row.setField(44, getBool(node, "in_beat"));
            row.setField(45, getBool(node, "in_range"));

            // 46-51. Misc
            row.setField(46, getString(node, "group_id"));
            row.setField(47, getString(node, "loginid"));
            row.setField(48, getString(node, "hash"));
            row.setField(49, getBool(node, "changed"));
            row.setField(50, getDouble(node, "nw"));
            row.setField(51, getDouble(node, "sales_value"));

            // 52-54. JSON Fields
            row.setField(52, getString(node, "extended_attributes"));
            row.setField(53, getString(node, "discount_info"));
            row.setField(54, getString(node, "order_details"));

            // 55. Ingestion time
            row.setField(55, getTimestamp(node, "ingestion_time"));
            log.info("row: {}", row);

            return row;
        }

        // Type helpers
        private StringData getString(ObjectNode node, String field) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode == null || fieldNode.isNull())
                return null;
            String value = fieldNode.asText();
            if (value == null || value.isEmpty() || "null".equals(value))
                return null;
            return StringData.fromString(value);
        }

        private Double getDouble(ObjectNode node, String field) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode == null || fieldNode.isNull())
                return 0.0;
            if (fieldNode.isNumber())
                return fieldNode.asDouble();
            return 0.0;
        }

        private Float getFloat(ObjectNode node, String field) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode == null || fieldNode.isNull())
                return 0.0f;
            if (fieldNode.isNumber())
                return (float) fieldNode.asDouble();
            return 0.0f;
        }

        private Integer getInt(ObjectNode node, String field) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode == null || fieldNode.isNull())
                return 0;
            if (fieldNode.isNumber())
                return fieldNode.asInt();
            return 0;
        }

        private Boolean getBool(ObjectNode node, String field) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode == null || fieldNode.isNull())
                return false;
            if (fieldNode.isBoolean())
                return fieldNode.asBoolean();
            return false;
        }

        private TimestampData getTimestamp(ObjectNode node, String field) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode == null || fieldNode.isNull())
                return null;

            if (fieldNode.isNumber()) {
                return TimestampData.fromEpochMillis(fieldNode.asLong());
            }

            String value = fieldNode.asText();
            if (value == null || value.trim().isEmpty() || "null".equalsIgnoreCase(value)) {
                return null;
            }

            return parseTimestampString(value);
        }

        private TimestampData parseTimestampString(String value) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(value, CUSTOM_FORMATTER);
                return TimestampData.fromLocalDateTime(ldt);
            } catch (DateTimeParseException ignored) {
            }

            try {
                LocalDateTime ldt = LocalDateTime.parse(value, WITH_MILLIS_FORMATTER);
                return TimestampData.fromLocalDateTime(ldt);
            } catch (DateTimeParseException ignored) {
            }

            try {
                LocalDateTime ldt = LocalDateTime.parse(value);
                return TimestampData.fromLocalDateTime(ldt);
            } catch (DateTimeParseException ignored) {
            }

            try {
                Instant instant = Instant.parse(value);
                return TimestampData.fromInstant(instant);
            } catch (DateTimeParseException ignored) {
            }

            try {
                OffsetDateTime odt = OffsetDateTime.parse(value);
                return TimestampData.fromInstant(odt.toInstant());
            } catch (DateTimeParseException ignored) {
            }

            return null;
        }
    }
}
