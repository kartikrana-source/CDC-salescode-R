# Kafka → Iceberg CDC Pipeline

A production-ready Flink application that reads order change events from Apache Kafka and writes them to Apache Iceberg tables stored on Amazon S3, using AWS Glue Catalog for metadata management.

## Architecture

```
┌─────────────┐     ┌──────────────────────────────────────────────────────────────┐     ┌─────────────┐
│             │     │                    Apache Flink Application                   │     │             │
│   Apache    │     │  ┌───────────┐   ┌─────────────┐   ┌──────────────────────┐  │     │   Amazon    │
│   Kafka     │────▶│  │ Kafka     │──▶│ Order       │──▶│ Iceberg Sink Builder │──┼────▶│   S3        │
│             │     │  │ Source    │   │ Transformer │   │ (RowData Mapper)     │  │     │ (Parquet)   │
└─────────────┘     │  └───────────┘   └─────────────┘   └──────────────────────┘  │     └─────────────┘
                    │                                              │                │           │
                    └──────────────────────────────────────────────┼────────────────┘           │
                                                                   │                            │
                                                         ┌─────────▼─────────┐                  │
                                                         │  AWS Glue Catalog │◀─────────────────┘
                                                         │  (Metadata)       │
                                                         └─────────▲─────────┘
                                                                   │
                                                         ┌─────────┴─────────┐
                                                         │  Amazon Athena    │
                                                         │  (SQL Queries)    │
                                                         └───────────────────┘
```

## Project Structure

```
src/main/java/com/salescode/
├── Main.java                          # Application entry point
├── config/
│   ├── AppConfig.java                 # Root configuration class
│   ├── ConfigLoader.java              # YAML configuration loader
│   ├── KafkaConfig.java               # Kafka connection settings
│   └── IcebergConfig.java             # Iceberg/Glue catalog settings
├── kafka/
│   └── KafkaSourceBuilder.java        # Builds Flink Kafka source
├── transformer/
│   └── OrderHeaderTransformer.java    # Maps Kafka JSON → 56 fields
├── sink/
│   └── IcebergSinkBuilder.java        # Builds Iceberg sink with RowData mapper
└── iceberg/
    ├── CreateIcebergTables.java       # Creates Iceberg table schema (standalone)
    ├── IcebergTableInitializer.java   # Ensures tables exist at startup
    └── IcebergUtil.java               # Glue catalog utilities
```

## Data Flow

### 1. Kafka Source
**File:** `kafka/KafkaSourceBuilder.java`

Reads JSON messages from Kafka topic `entity-change-events-1`:
```json
{
  "lob": "simamyuat",
  "features": [
    {
      "id": "26838eb2b-1j",
      "orderNumber": "26838eB2B",
      "status": "PENDING",
      "billAmount": 615.8,
      "orderDetails": [...]
    }
  ]
}
```

### 2. Transformer
**File:** `transformer/OrderHeaderTransformer.java`

Extracts each order from the `features[]` array and maps to 56 columns:
- Flattens nested JSON structure
- Converts field names (camelCase → snake_case)
- Adds `ingestion_time` timestamp for tracking latest records

### 3. Iceberg Sink
**File:** `sink/IcebergSinkBuilder.java`

Converts transformed JSON to Flink `RowData` and writes to Iceberg:
- Handles type conversions (String → StringData, timestamps → TimestampData)
- Writes Parquet files to S3
- Commits metadata to Glue Catalog

## Partitioning Strategy

**File:** `iceberg/CreateIcebergTables.java`

Multi-level partitioning optimized for production queries:

```
lob (identity) → bucket(16, id) → year(creation_time) → month(creation_time)
```

### S3 Folder Structure
```
s3://warehouse/ck_orders/
├── lob=simamy/
│   ├── id_bucket=0/
│   │   ├── creation_time_year=2026/
│   │   │   ├── creation_time_month=01/
│   │   │   │   └── *.parquet
│   │   │   └── creation_time_month=02/
│   │   │       └── *.parquet
│   ├── id_bucket=1/
│   │   └── ...
│   └── id_bucket=15/
├── lob=simasg/
│   └── ...
└── lob=niine/
    └── ...
```

### Partition Benefits\n| Level | Column | Purpose |\n|-------|--------|---------|
| 1 | `lob` | Isolate data by Line of Business |
| 2 | `bucket(id, 16)` | Even data distribution across 16 buckets |
| 3 | `year(creation_time)` | Fast time-range queries |
| 4 | `month(creation_time)` | Fine-grained monthly partitions |

## Table Schema (56 columns)

| # | Column | Type | Description |
|---|--------|------|-------------|
| 1 | id | STRING | Primary key (order ID) |
| 2-3 | active_status, active_status_reason | STRING | Order status flags |
| 4-8 | created_by, creation_time, etc. | STRING/TIMESTAMP | Audit fields |
| 9-22 | lob, version, amounts, quantities | Various | Business fields |
| 23-27 | order_number, reference_number, etc. | STRING | Order identifiers |
| 28-32 | location_hierarchy, outletcode, etc. | STRING | Location data |
| 33-40 | gps, type, status, channel | STRING | Additional metadata |
| 41-42 | delivery_date, sales_date | TIMESTAMP | Date fields |
| 43-52 | beat info, misc fields | Various | Additional fields |
| 53-55 | extended_attributes, discount_info, order_details | STRING (JSON) | Nested data as JSON |
| 56 | **ingestion_time** | TIMESTAMP | When record was ingested (for finding latest) |

## Configuration

**File:** `src/main/resources/application.yaml`

```yaml
kafka:
  brokers: "164.52.202.184:9092"
  topic: "entity-change-events-1"
  groupId: "flink-cdc-group"
  readFromEarliest: false

iceberg:
  warehouse: "s3://sc-developer-bucket/iceberg_db_test.db"
  catalog-name: "glue_catalog"
  catalog-type: "glue"
  io-impl: "org.apache.iceberg.aws.s3.S3FileIO"
  aws-region: "ap-south-1"
  database: "iceberg_db_test"
  table: "ck_orders"
```

## Running the Application

### Prerequisites
```bash
# 1. Login to AWS SSO
aws sso login --profile default
```

### Build

**For local development:**
```bash
mvn clean package -DskipTests
```

**For AWS Managed Flink deployment:**
```bash
mvn clean package -DskipTests -Paws
```

| Profile | JAR Size | Use Case |
|---------|----------|----------|
| `local` (default) | ~490MB | Running locally or from IDE |
| `aws` (`-Paws`) | ~107MB | Deploying to AWS Managed Flink |

### Run Locally
```bash
# Using Maven
mvn exec:java -Dexec.mainClass="com.salescode.Main"

# Or using JAR
java -jar target/flink-iceberg-pipeline-1.0-SNAPSHOT.jar
```

### Deploy to AWS Managed Flink
1. Build with AWS profile: `mvn clean package -DskipTests -Paws`
2. Upload `target/flink-iceberg-pipeline-1.0-SNAPSHOT.jar` to S3
3. Create AWS Managed Flink application pointing to the S3 JAR
4. Ensure the IAM execution role has permissions for Glue, S3, and Kafka

## Querying Data in Athena

### Get all orders (with partition pruning)
```sql
-- Scans only simamy partition
SELECT * FROM iceberg_db_test.ck_orders 
WHERE lob = 'simamy' 
LIMIT 100;
```

### Query specific LOB + time range (highly optimized)
```sql
-- Scans: 1 LOB + 1 month = minimal data scan
SELECT * FROM iceberg_db_test.ck_orders 
WHERE lob = 'simamy' 
  AND creation_time BETWEEN TIMESTAMP '2026-01-01 00:00:00' 
                        AND TIMESTAMP '2026-01-31 23:59:59';
```

### Get latest status for each order (using ingestion_time)
```sql
WITH ranked AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingestion_time DESC) as rn
    FROM iceberg_db_test.ck_orders
    WHERE lob = 'simamy'  -- Add LOB filter for performance
)
SELECT * FROM ranked WHERE rn = 1;
```

### Orders by status per LOB
```sql
SELECT lob, status, COUNT(*) as count
FROM iceberg_db_test.ck_orders
GROUP BY lob, status
ORDER BY lob, count DESC;
```

## Key Features

- **Multi-LOB Partitioning**: Data isolated by Line of Business with automatic folder separation
- **Optimized Queries**: 4-level partitioning (LOB/bucket/year/month) for efficient partition pruning
- **CDC Support**: Append-only mode with `ingestion_time` for tracking latest records
- **AWS Integration**: Glue Catalog + S3 + Athena compatible
- **Checkpointing**: 30-second checkpoint interval for data durability
- **Schema Evolution**: Iceberg v2 format supports schema evolution
- **Compression**: ZSTD compression for efficient storage

## Technologies

| Component | Version |
|-----------|---------|
| Apache Flink | 1.19.0 |
| Apache Iceberg | 1.6.1 |
| Apache Kafka Client | 3.4.0 |
| AWS Glue Catalog | - |
| Java | 11 |
