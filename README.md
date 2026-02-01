# Kafka → Iceberg CDC Pipeline (Multi-LOB with UPSERT)

A production-ready Flink application that reads order change events from Apache Kafka and writes them to Apache Iceberg tables stored on Amazon S3, using AWS Glue Catalog for metadata management. Features **dynamic LOB-based routing** and **UPSERT mode** for handling updates.

## Architecture

```
┌─────────────┐     ┌──────────────────────────────────────────────────────────────┐     ┌──────────────────────┐
│             │     │                    Apache Flink Application                   │     │                      │
│   Apache    │     │  ┌───────────┐   ┌─────────────┐   ┌───────────────────────┐ │     │   Amazon S3          │
│   Kafka     │────▶│  │ Kafka     │──▶│ Order       │──▶│ Dynamic LOB Sink      │─┼────▶│   ck_orders_niineuat │
│             │     │  │ Source    │   │ Transformer │   │ (Routes by LOB field) │ │     │   ck_orders_simamyuat│
└─────────────┘     │  └───────────┘   └─────────────┘   └───────────────────────┘ │     │   (Parquet files)    │
                    │                                              │                │     └──────────────────────┘
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

## Key Features

| Feature | Description |
|---------|-------------|
| **Dynamic LOB Routing** | Each LOB gets its own table (`ck_orders_{lob}`) for data isolation |
| **UPSERT Mode** | Updates replace existing records using `id` + `creation_time` as equality fields |
| **Partition Optimized** | `bucket(id, 16)` + `year(creation_time)` for efficient queries |
| **Zero-Code Onboarding** | Add new LOBs via `application.yaml` - no code changes needed |
| **Time Travel** | Query historical data using Iceberg snapshots |
| **ZSTD Compression** | Parquet files compressed with ZSTD for efficient storage |

## Project Structure

```
src/main/java/com/salescode/
├── Main.java                          # Application entry point
├── config/
│   ├── AppConfig.java                 # Root configuration class
│   ├── ConfigLoader.java              # YAML configuration loader
│   ├── KafkaConfig.java               # Kafka connection settings
│   └── IcebergConfig.java             # Iceberg/Glue catalog settings + known-lobs
├── kafka/
│   └── KafkaSourceBuilder.java        # Builds Flink Kafka source
├── transformer/
│   └── OrderHeaderTransformer.java    # Maps Kafka JSON → 56 fields
├── sink/
│   └── DynamicLobSink.java            # LOB routing + Iceberg sinks with UPSERT
└── iceberg/
    ├── CreateIcebergTables.java       # Creates Iceberg table schema
    └── IcebergUtil.java               # Glue catalog utilities
```

---

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
  known-lobs:
    - niineuat
    - simamyuat
```

### Adding a New LOB

Simply add to `known-lobs` and restart:
```yaml
known-lobs:
  - niineuat
  - simamyuat
  - newlob       # ← Add here
```

The table `ck_orders_newlob` will be created automatically.

---

## Running the Application

### Prerequisites
```bash
# Login to AWS SSO
aws sso login --profile salescode-uat
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
| `local` (default) | ~176 MB | Running locally or from IDE |
| `aws` (`-Paws`) | ~110 MB | Deploying to AWS Managed Flink |

> The `-Paws` profile marks Flink core dependencies as `provided` since AWS Managed Flink already includes them.

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

---

## Querying Data in Athena

### Basic Queries

**Get all orders for a LOB:**
```sql
SELECT * FROM iceberg_db_test.ck_orders_simamyuat LIMIT 100;
```

**Query by time range:**
```sql
SELECT * FROM iceberg_db_test.ck_orders_simamyuat 
WHERE creation_time BETWEEN TIMESTAMP '2026-01-01' AND TIMESTAMP '2026-01-31';
```

**Verify UPSERT worked (no duplicates):**
```sql
SELECT id, status, creation_time, COUNT(*) as cnt
FROM iceberg_db_test.ck_orders_simamyuat
GROUP BY id, status, creation_time
HAVING COUNT(*) > 1;  -- Should return 0 rows
```

**Orders by status:**
```sql
SELECT status, COUNT(*) as count
FROM iceberg_db_test.ck_orders_niineuat
GROUP BY status
ORDER BY count DESC;
```

---

## Time Travel Queries

Iceberg maintains a history of all changes (snapshots), allowing you to query data as it existed at any point in time.

### View Snapshot History

```sql
SELECT
    snapshot_id,
    committed_at,
    operation
FROM AwsDataCatalog.iceberg_db_test."ck_orders_niineuat$snapshots"
ORDER BY committed_at DESC;
```

### Query Data at a Specific Snapshot

```sql
SELECT id, status, creation_time
FROM AwsDataCatalog.iceberg_db_test.ck_orders_niineuat
FOR VERSION AS OF 7764363427711567836;
```

### Query Data at a Specific Timestamp

```sql
SELECT id, status, creation_time
FROM AwsDataCatalog.iceberg_db_test.ck_orders_niineuat
FOR TIMESTAMP AS OF TIMESTAMP '2026-01-31 12:00:00';
```

### View Table Metadata

```sql
-- View all table files
SELECT * FROM AwsDataCatalog.iceberg_db_test."ck_orders_niineuat$files";

-- View table history
SELECT * FROM AwsDataCatalog.iceberg_db_test."ck_orders_niineuat$history";

-- View partitions
SELECT * FROM AwsDataCatalog.iceberg_db_test."ck_orders_niineuat$partitions";
```

---

## UPSERT Behavior

When an order update arrives:

| Step | Record | Result |
|------|--------|--------|
| Day 1 | `id=A, creation_time=2026-01-15, status=PENDING` | Inserted |
| Day 3 | `id=A, creation_time=2026-01-15, status=CONFIRMED` | Updated (replaces) |

**Only 1 row per `id` + `creation_time`** - no duplicates!

> **Important**: `creation_time` must never change for an order. If it changes, a new row will be created instead of an update.

---

## Partitioning Strategy

```
bucket(id, 16) → year(creation_time)
```

| Partition | Purpose |
|-----------|---------|
| `bucket(id, 16)` | Even data distribution across 16 buckets |
| `year(creation_time)` | Fast time-range queries + required for UPSERT |

---

## Technologies

| Component | Version |
|-----------|---------|
| Apache Flink | 1.19.0 |
| Apache Iceberg | 1.6.1 |
| Apache Avro | 1.11.3 |
| Apache Kafka Client | 3.4.0 |
| Hadoop Common | 3.3.5 |
| AWS Glue Catalog | - |
| Java | 11 |
