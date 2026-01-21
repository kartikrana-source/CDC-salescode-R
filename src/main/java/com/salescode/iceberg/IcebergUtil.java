package com.salescode.iceberg;

import com.salescode.config.IcebergConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for Iceberg operations using AWS Glue Catalog.
 * Uses AWS SDK default credential chain for authentication.
 */
@Slf4j
public class IcebergUtil {

    private IcebergUtil() {
        // Prevent instantiation
    }

    /**
     * Create a Glue Catalog Loader for Iceberg operations.
     */
    public static CatalogLoader glueCatalogLoader(IcebergConfig icebergConfig) {
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", icebergConfig.getWarehouse());
        properties.put("io-impl", icebergConfig.getIoImpl());

        log.info("Creating Glue CatalogLoader with warehouse: {}", icebergConfig.getWarehouse());

        return CatalogLoader.custom(
                "glue",
                properties,
                new Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog");
    }

    /**
     * Create a TableLoader for the orders table using Glue Catalog.
     * This is the preferred method for Athena/Glue integration.
     */
    public static TableLoader ordersTableLoader(IcebergConfig icebergConfig) {
        CatalogLoader catalogLoader = glueCatalogLoader(icebergConfig);
        TableIdentifier tableId = TableIdentifier.of(icebergConfig.getDatabase(), icebergConfig.getTable());
        log.info("Loading table from Glue Catalog: {}.{}", icebergConfig.getDatabase(), icebergConfig.getTable());
        return TableLoader.fromCatalog(catalogLoader, tableId);
    }
}