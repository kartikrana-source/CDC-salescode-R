package com.salescode.iceberg;

import com.salescode.config.AppConfig;
import com.salescode.config.ConfigLoader;
import com.salescode.config.IcebergConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Map;

/**
 * Creates Iceberg tables matching the MySQL ck_orders schema.
 * Uses AWS Glue Catalog for metadata storage.
 * 
 * Schema: 56 columns (55 business + 1 ingestion_time for tracking)
 * Partition: None (as per requirements)
 */
@Slf4j
public class CreateIcebergTables {

    // Iceberg Table Properties (Athena-compatible)
    private static final Map<String, String> TABLE_PROPERTIES = new HashMap<>() {
        {
            put("format-version", "2");
            put("write.format.default", "parquet");
            put("write.parquet.compression-codec", "zstd");
        }
    };

    public static void main(String[] args) {
        AppConfig appConfig = ConfigLoader.loadConfig("application.yaml");
        IcebergConfig icebergConfig = appConfig.getIceberg();

        log.info("Using warehouse: {}", icebergConfig.getWarehouse());
        log.info("Using AWS region: {}", icebergConfig.getAwsRegion());

        // Initialize GlueCatalog
        GlueCatalog catalog = new GlueCatalog();
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", icebergConfig.getWarehouse());
        properties.put("io-impl", icebergConfig.getIoImpl());
        catalog.initialize("glue", properties);

        createOrdersTable(catalog, icebergConfig);

        log.info("✔ All Iceberg tables are ready in AWS Glue Catalog.");
    }

    /**
     * Create ck_orders table matching MySQL schema (56 columns including
     * ingestion_time).
     */
    public static void createOrdersTable(Catalog catalog, IcebergConfig icebergConfig) {
        String database = icebergConfig.getDatabase();
        String table = icebergConfig.getTable();
        TableIdentifier tableId = TableIdentifier.of(database, table);

        if (catalog.tableExists(tableId)) {
            log.info("✔ Table {}.{} already exists. Skipping.", database, table);
            return;
        }

        Schema schema = new Schema(
                // Primary Key
                Types.NestedField.required(1, "id", Types.StringType.get()),

                // Status Fields
                Types.NestedField.optional(2, "active_status", Types.StringType.get()),
                Types.NestedField.optional(3, "active_status_reason", Types.StringType.get()),

                // Audit Fields
                Types.NestedField.optional(4, "created_by", Types.StringType.get()),
                Types.NestedField.optional(5, "creation_time", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(6, "last_modified_time", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(7, "modified_by", Types.StringType.get()),
                Types.NestedField.optional(8, "system_time", Types.TimestampType.withoutZone()),

                // Business Fields
                Types.NestedField.optional(9, "lob", Types.StringType.get()),
                Types.NestedField.optional(10, "version", Types.IntegerType.get()),
                Types.NestedField.optional(11, "source", Types.StringType.get()),
                Types.NestedField.optional(12, "bill_amount", Types.DoubleType.get()),
                Types.NestedField.optional(13, "net_amount", Types.DoubleType.get()),
                Types.NestedField.optional(14, "total_amount", Types.DoubleType.get()),
                Types.NestedField.optional(15, "total_initial_amt", Types.DoubleType.get()),
                Types.NestedField.optional(16, "total_initial_quantity", Types.FloatType.get()),
                Types.NestedField.optional(17, "total_mrp", Types.DoubleType.get()),
                Types.NestedField.optional(18, "total_quantity", Types.FloatType.get()),
                Types.NestedField.optional(19, "normalized_quantity", Types.FloatType.get()),
                Types.NestedField.optional(20, "initial_normalized_quantity", Types.FloatType.get()),
                Types.NestedField.optional(21, "normalized_volume", Types.FloatType.get()),
                Types.NestedField.optional(22, "line_count", Types.IntegerType.get()),

                // Order Identifiers
                Types.NestedField.optional(23, "order_number", Types.StringType.get()),
                Types.NestedField.optional(24, "reference_number", Types.StringType.get()),
                Types.NestedField.optional(25, "reference_order_number", Types.StringType.get()),
                Types.NestedField.optional(26, "remarks", Types.StringType.get()),
                Types.NestedField.optional(27, "ship_id", Types.StringType.get()),

                // Location & Hierarchy
                Types.NestedField.optional(28, "location_hierarchy", Types.StringType.get()),
                Types.NestedField.optional(29, "outletcode", Types.StringType.get()),
                Types.NestedField.optional(30, "supplierid", Types.StringType.get()),
                Types.NestedField.optional(31, "hierarchy", Types.StringType.get()),
                Types.NestedField.optional(32, "user_hierarchy", Types.StringType.get()),

                // GPS
                Types.NestedField.optional(33, "gps_latitude", Types.StringType.get()),
                Types.NestedField.optional(34, "gps_longitude", Types.StringType.get()),

                // Type & Status
                Types.NestedField.optional(35, "type", Types.StringType.get()),
                Types.NestedField.optional(36, "sub_type", Types.StringType.get()),
                Types.NestedField.optional(37, "status", Types.StringType.get()),
                Types.NestedField.optional(38, "status_reason", Types.StringType.get()),
                Types.NestedField.optional(39, "processing_status", Types.StringType.get()),
                Types.NestedField.optional(40, "channel", Types.StringType.get()),

                // Dates
                Types.NestedField.optional(41, "delivery_date", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(42, "sales_date", Types.TimestampType.withoutZone()),

                // Beat Info
                Types.NestedField.optional(43, "beat", Types.StringType.get()),
                Types.NestedField.optional(44, "beat_name", Types.StringType.get()),
                Types.NestedField.optional(45, "in_beat", Types.BooleanType.get()),
                Types.NestedField.optional(46, "in_range", Types.BooleanType.get()),

                // Misc
                Types.NestedField.optional(47, "group_id", Types.StringType.get()),
                Types.NestedField.optional(48, "loginid", Types.StringType.get()),
                Types.NestedField.optional(49, "hash", Types.StringType.get()),
                Types.NestedField.optional(50, "changed", Types.BooleanType.get()),
                Types.NestedField.optional(51, "nw", Types.DoubleType.get()),
                Types.NestedField.optional(52, "sales_value", Types.DoubleType.get()),

                // JSON Fields (stored as strings for Athena compatibility)
                Types.NestedField.optional(53, "extended_attributes", Types.StringType.get()),
                Types.NestedField.optional(54, "discount_info", Types.StringType.get()),
                Types.NestedField.optional(55, "order_details", Types.StringType.get()),

                // Ingestion tracking - use this to find latest record per order
                Types.NestedField.required(56, "ingestion_time", Types.TimestampType.withoutZone()));

        // Multi-level partitioning:
        // lob (identity) → bucket(16, id) → y=YYYY / m=MM from creation_time
        // Result:
        // s3://warehouse/ck_orders/lob=FMCG/id_bucket=0/creation_time_year=2026/creation_time_month=01/
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                .identity("lob") // Level 1: Line of Business
                .bucket("id", 16) // Level 2: Bucket by entity ID (16 buckets)
                .year("creation_time")
                .build();
              // Level 3: Year partition (y=YYYY)
              // Level 4: Month partition (m=MM)

        catalog.createTable(tableId, schema, partitionSpec, TABLE_PROPERTIES);
        log.info("✔ Created Iceberg v2 table: {}.{} (56 columns, partitioned by LOB/bucket(16,id)/year/month)",
                database, table);
    }
}
