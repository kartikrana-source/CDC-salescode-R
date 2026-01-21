package com.salescode.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Builder for creating Iceberg sinks for Order Headers.
 * Maps 55 fields from ObjectNode to RowData for Iceberg ingestion.
 */
@Slf4j
public class IcebergSinkBuilder {

    private static final DateTimeFormatter CUSTOM_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter WITH_MILLIS_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private IcebergSinkBuilder() {
        // Prevent instantiation
    }

    /**
     * Create an Iceberg sink for Order Headers (ck_orders table).
     */
    public static DataStreamSink<Void> createOrderHeaderSink(
            DataStream<ObjectNode> stream,
            TableLoader tableLoader) {

        DataStream<RowData> rowStream = stream.map(new OrderHeaderMapper());

        return FlinkSink.forRowData(rowStream)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .append();
    }

    /**
     * Maps ObjectNode (55 fields) to RowData for Iceberg sink.
     */
    private static class OrderHeaderMapper implements MapFunction<ObjectNode, RowData> {

        @Override
        public RowData map(ObjectNode node) throws Exception {
            GenericRowData row = new GenericRowData(56);

            // 0. id (required)
            row.setField(0, getString(node, "id"));

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

            // 55. Ingestion time - when this record was processed
            row.setField(55, getTimestamp(node, "ingestion_time"));

            return row;
        }

        // ============================================================
        // Type Conversion Helpers - using instance methods
        // ============================================================

        private StringData getString(ObjectNode node, String field) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode == null || fieldNode.isNull()) {
                return null;
            }
            String value = fieldNode.asText();
            if (value == null || value.isEmpty() || "null".equals(value)) {
                return null;
            }
            return StringData.fromString(value);
        }

        private Double getDouble(ObjectNode node, String field) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode == null || fieldNode.isNull()) {
                return 0.0;
            }
            if (fieldNode.isNumber()) {
                return fieldNode.asDouble();
            }
            return 0.0;
        }

        private Float getFloat(ObjectNode node, String field) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode == null || fieldNode.isNull()) {
                return 0.0f;
            }
            if (fieldNode.isNumber()) {
                return (float) fieldNode.asDouble();
            }
            return 0.0f;
        }

        private Integer getInt(ObjectNode node, String field) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode == null || fieldNode.isNull()) {
                return 0;
            }
            if (fieldNode.isNumber()) {
                return fieldNode.asInt();
            }
            return 0;
        }

        private Boolean getBool(ObjectNode node, String field) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode == null || fieldNode.isNull()) {
                return false;
            }
            if (fieldNode.isBoolean()) {
                return fieldNode.asBoolean();
            }
            return false;
        }

        /**
         * Parse timestamp from various formats.
         * Returns null for null/empty values (Iceberg handles optional timestamps).
         */
        private TimestampData getTimestamp(ObjectNode node, String field) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode == null || fieldNode.isNull()) {
                return null;
            }

            // Handle epoch millis (number)
            if (fieldNode.isNumber()) {
                long epochMillis = fieldNode.asLong();
                return TimestampData.fromEpochMillis(epochMillis);
            }

            // Handle string timestamp
            String value = fieldNode.asText();
            if (value == null || value.trim().isEmpty() || "null".equalsIgnoreCase(value)) {
                return null;
            }

            return parseTimestampString(value, field);
        }

        private TimestampData parseTimestampString(String value, String field) {
            // 1. Try custom format: "yyyy-MM-dd HH:mm:ss"
            try {
                LocalDateTime ldt = LocalDateTime.parse(value, CUSTOM_FORMATTER);
                return TimestampData.fromLocalDateTime(ldt);
            } catch (DateTimeParseException e) {
                // continue
            }

            // 2. Try with milliseconds: "yyyy-MM-dd HH:mm:ss.SSS"
            try {
                LocalDateTime ldt = LocalDateTime.parse(value, WITH_MILLIS_FORMATTER);
                return TimestampData.fromLocalDateTime(ldt);
            } catch (DateTimeParseException e) {
                // continue
            }

            // 3. Try ISO LocalDateTime: "yyyy-MM-ddTHH:mm:ss"
            try {
                LocalDateTime ldt = LocalDateTime.parse(value);
                return TimestampData.fromLocalDateTime(ldt);
            } catch (DateTimeParseException e) {
                // continue
            }

            // 4. Try ISO Instant: "yyyy-MM-ddTHH:mm:ssZ"
            try {
                Instant instant = Instant.parse(value);
                return TimestampData.fromInstant(instant);
            } catch (DateTimeParseException e) {
                // continue
            }

            // 5. Try OffsetDateTime: "yyyy-MM-ddTHH:mm:ss+00:00"
            try {
                OffsetDateTime odt = OffsetDateTime.parse(value);
                return TimestampData.fromInstant(odt.toInstant());
            } catch (DateTimeParseException e) {
                // continue
            }

            log.warn("Could not parse timestamp '{}' for field '{}', returning null", value, field);
            return null;
        }
    }
}
