package com.salescode.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.Schema;

import java.util.List;

/**
 * Converts ObjectNode (JSON) to Flink RowData based on an Iceberg Schema.
 * This mapper dynamically handles field conversion based on the schema's
 * logical types.
 */
public class ObjectNodeToRowData implements MapFunction<ObjectNode, RowData> {

    private final Schema schema;
    private final RowType rowType;

    public ObjectNodeToRowData(Schema schema) {
        this.schema = schema;
        this.rowType = FlinkSchemaUtil.convert(schema);
    }

    @Override
    public RowData map(ObjectNode json) throws Exception {
        List<RowType.RowField> fields = rowType.getFields();
        GenericRowData row = new GenericRowData(fields.size());

        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            String fieldName = field.getName();
            LogicalType logicalType = field.getType();

            // Get the JSON value for this field
            JsonNode jsonValue = json.get(fieldName);

            // Convert based on the logical type
            Object convertedValue = convertValue(jsonValue, logicalType);
            row.setField(i, convertedValue);
        }

        return row;
    }

    /**
     * Convert a JSON value to the appropriate Flink type based on the logical type
     */
    private Object convertValue(JsonNode jsonValue, LogicalType logicalType) {
        if (jsonValue == null || jsonValue.isNull()) {
            return null;
        }

        switch (logicalType.getTypeRoot()) {
            case VARCHAR:
            case CHAR:
                return StringData.fromString(jsonValue.asText());

            case BOOLEAN:
                return jsonValue.asBoolean();

            case TINYINT:
                return (byte) jsonValue.asInt();

            case SMALLINT:
                return (short) jsonValue.asInt();

            case INTEGER:
                return jsonValue.asInt();

            case BIGINT:
                return jsonValue.asLong();

            case FLOAT:
                return (float) jsonValue.asDouble();

            case DOUBLE:
                return jsonValue.asDouble();

            case DECIMAL:
                return jsonValue.decimalValue();

            case DATE:
                // Assuming the JSON contains date as string or epoch days
                // You may need to adjust this based on your actual data format
                return jsonValue.asInt();

            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Assuming the JSON contains timestamp as long (milliseconds)
                // You may need to adjust this based on your actual data format
                return jsonValue.asLong();

            default:
                // For unsupported types, return null or throw an exception
                return null;
        }
    }
}
