package com.salescode.iceberg;

import com.salescode.config.AppConfig;
import com.salescode.config.IcebergConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;

/**
 * Initializes Iceberg tables at application startup.
 * Ensures tables exist with proper CDC versioning schema.
 */
@Slf4j
public class IcebergTableInitializer {

    // ============================================================
    // Config-aware method (preferred)
    // ============================================================

    public static void ensureTablesExist(AppConfig config) {
        log.info("Initializing Iceberg tables...");

        IcebergConfig icebergConfig = config.getIceberg();

        log.info("Using warehouse: {}", icebergConfig.getWarehouse());
        log.info("Using catalog type: {}", icebergConfig.getCatalogType());

        try {
            CatalogLoader catalogLoader = IcebergUtil.glueCatalogLoader(icebergConfig);
            Catalog catalog = catalogLoader.loadCatalog();

            CreateIcebergTables.createOrdersTable(catalog, icebergConfig);

            log.info("✔ Iceberg tables initialization complete (Glue Catalog).");
        } catch (Exception e) {
            log.error("✖ Failed to initialize Iceberg tables: {}", e.getMessage(), e);
            throw new RuntimeException("Iceberg table initialization failed", e);
        }
    }

}
