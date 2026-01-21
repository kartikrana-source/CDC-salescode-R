package com.salescode.transformer;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

/**
 * Transforms Kafka CDC events into Order Header records.
 * Maps all 55 fields from Kafka payload to match Iceberg schema.
 */
@Slf4j
public class OrderHeaderTransformer implements FlatMapFunction<ObjectNode, ObjectNode> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void flatMap(ObjectNode event, Collector<ObjectNode> out) throws Exception {
        ArrayNode features = (ArrayNode) event.get("features");

        if (features == null || features.isEmpty()) {
            log.warn("No features[] list found in message!");
            return;
        }

        for (int i = 0; i < features.size(); i++) {
            ObjectNode feature = (ObjectNode) features.get(i);
            ObjectNode row = mapper.createObjectNode();

            // 1. id (required)
            row.put("id", text(feature, "id"));

            // 2-3. Status Fields
            row.put("active_status", text(feature, "activeStatus"));
            row.put("active_status_reason", text(feature, "activeStatusReason"));

            // 4-8. Audit Fields
            row.put("created_by", text(feature, "createdBy"));
            row.put("creation_time", text(feature, "creationTime"));
            row.put("last_modified_time", text(feature, "lastModifiedTime"));
            row.put("modified_by", text(feature, "modifiedBy"));
            row.put("system_time", text(feature, "systemTime"));

            // 9-11. Business Fields
            row.put("lob", text(feature, "lob"));
            row.put("version", intVal(feature, "version"));
            row.put("source", text(feature, "source"));

            // 12-22. Amount and Quantity Fields
            row.put("bill_amount", dbl(feature, "billAmount"));
            row.put("net_amount", dbl(feature, "netAmount"));
            row.put("total_amount", dbl(feature, "totalAmount"));
            row.put("total_initial_amt", dbl(feature, "totalInitialAmt"));
            row.put("total_initial_quantity", flt(feature, "totalInitialQuantity"));
            row.put("total_mrp", dbl(feature, "totalMrp"));
            row.put("total_quantity", flt(feature, "totalQuantity"));
            row.put("normalized_quantity", flt(feature, "normalizedQuantity"));
            row.put("initial_normalized_quantity", flt(feature, "initialNormalizedQuantity"));
            row.put("normalized_volume", flt(feature, "normalizedVolume"));
            row.put("line_count", intVal(feature, "lineCount"));

            // 23-27. Order Identifiers
            row.put("order_number", text(feature, "orderNumber"));
            row.put("reference_number", text(feature, "referenceNumber"));
            row.put("reference_order_number", text(feature, "referenceOrderNumber"));
            row.put("remarks", text(feature, "remarks"));
            row.put("ship_id", text(feature, "shipId"));

            // 28-32. Location & Hierarchy
            row.put("location_hierarchy", textOrDefault(feature, "locationHierarchy", ""));
            row.put("outletcode", textOrDefault(feature, "outletCode", ""));
            row.put("supplierid", textOrDefault(feature, "supplier", ""));
            row.put("hierarchy", textOrDefault(feature, "hierarchy", ""));
            row.put("user_hierarchy", text(feature, "supplierHierarchy"));

            // 33-34. GPS
            row.put("gps_latitude", text(feature, "gpsLatitude"));
            row.put("gps_longitude", text(feature, "gpsLongitude"));

            // 35-40. Type & Status
            row.put("type", text(feature, "type"));
            row.put("sub_type", text(feature, "subType"));
            row.put("status", textOrDefault(feature, "status", "PENDING"));
            row.put("status_reason", text(feature, "statusReason"));
            row.put("processing_status", text(feature, "processingStatus"));
            row.put("channel", text(feature, "channel"));

            // 41-42. Dates
            row.put("delivery_date", text(feature, "deliveryDate"));
            row.put("sales_date", text(feature, "salesDate"));

            // 43-46. Beat Info
            row.put("beat", text(feature, "beat"));
            row.put("beat_name", text(feature, "beatName"));
            row.put("in_beat", bool(feature, "inBeat"));
            row.put("in_range", bool(feature, "inRange"));

            // 47-52. Misc
            row.put("group_id", text(feature, "groupId"));
            row.put("loginid", text(feature, "loginId"));
            row.put("hash", (String) null);
            row.put("changed", bool(feature, "changed"));
            row.put("nw", dbl(feature, "nw"));
            row.put("sales_value", dbl(feature, "salesValue"));

            // 53-55. JSON Fields (stored as strings)
            row.put("extended_attributes", jsonString(feature, "extendedAttributes"));
            row.put("discount_info", jsonString(feature, "discountInfo"));
            row.put("order_details", jsonString(feature, "orderDetails"));

            // 56. Ingestion time - current timestamp when record is processed
            row.put("ingestion_time", System.currentTimeMillis());

            out.collect(row);
        }
    }

    // Helper: Extract text field
    private String text(ObjectNode n, String f) {
        return n.has(f) && !n.get(f).isNull() ? n.get(f).asText() : null;
    }

    // Helper: Extract text with default value
    private String textOrDefault(ObjectNode n, String f, String defaultValue) {
        String value = text(n, f);
        return value != null ? value : defaultValue;
    }

    // Helper: Extract double field
    private double dbl(ObjectNode n, String f) {
        return n.has(f) && n.get(f).isNumber() ? n.get(f).asDouble() : 0.0;
    }

    // Helper: Extract float field
    private float flt(ObjectNode n, String f) {
        return n.has(f) && n.get(f).isNumber() ? (float) n.get(f).asDouble() : 0.0f;
    }

    // Helper: Extract integer field
    private int intVal(ObjectNode n, String f) {
        return n.has(f) && n.get(f).isNumber() ? n.get(f).asInt() : 0;
    }

    // Helper: Extract boolean field
    private boolean bool(ObjectNode n, String f) {
        return n.has(f) && n.get(f).isBoolean() && n.get(f).asBoolean();
    }

    // Helper: Convert node to JSON string
    private String jsonString(ObjectNode n, String f) {
        return n.has(f) && !n.get(f).isNull() ? n.get(f).toString() : null;
    }
}
