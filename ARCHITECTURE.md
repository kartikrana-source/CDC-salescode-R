# CDC Pipeline Architecture - Dynamic LOB Routing

## High-Level Flow

```mermaid
flowchart TB
    subgraph Source["📥 Data Source"]
        K[("Kafka Topic<br/>entity-change-events-1")]
    end

    subgraph Flink["⚡ Flink Processing Engine"]
        KS["KafkaSource<br/><i>KafkaSourceBuilder.java</i>"]
        TR["OrderHeaderTransformer<br/><i>Extracts root LOB</i>"]
        RT["LobRouterFunction<br/><i>Routes by LOB field</i>"]
        
        subgraph SideOutputs["Side Outputs"]
            SO1["niineuat<br/>stream"]
            SO2["simamyuat<br/>stream"]
            SO3["unknown<br/>stream"]
        end
        
        subgraph Mappers["RowData Mappers"]
            M1["OrderHeaderMapper"]
            M2["OrderHeaderMapper"]
        end
    end

    subgraph Sink["📤 Iceberg Sinks"]
        S1["FlinkSink<br/>ck_orders_niineuat"]
        S2["FlinkSink<br/>ck_orders_simamyuat"]
    end

    subgraph Storage["☁️ AWS S3 + Glue"]
        T1[("ck_orders_niineuat<br/><i>Parquet files</i>")]
        T2[("ck_orders_simamyuat<br/><i>Parquet files</i>")]
        G["AWS Glue Catalog<br/><i>Metadata</i>"]
    end

    K --> KS
    KS --> TR
    TR --> RT
    RT -->|lob=niineuat| SO1
    RT -->|lob=simamyuat| SO2
    RT -->|unknown lob| SO3
    SO1 --> M1 --> S1 --> T1
    SO2 --> M2 --> S2 --> T2
    T1 & T2 --> G
    SO3 -->|log warning| X[("⚠️ Skipped")]

    style Source fill:#e1f5fe
    style Flink fill:#fff3e0
    style Sink fill:#e8f5e9
    style Storage fill:#fce4ec
```

---

## Detailed Data Transformation

```mermaid
flowchart LR
    subgraph Input["Raw Kafka Message"]
        RAW["
        {
          <b>lob: 'niineuat'</b> ← Used
          features: [{
            lob: 'simamyuat' ← Ignored
            id: '26885eb2b-1j'
            billAmount: 615.8
            ...55 fields
          }]
        }
        "]
    end

    subgraph Transform["OrderHeaderTransformer"]
        T1["Extract root LOB"]
        T2["Flatten features[]"]
        T3["Map 55 fields"]
    end

    subgraph Output["Transformed Record"]
        OUT["
        {
          id: '26885eb2b-1j'
          <b>lob: 'niineuat'</b>
          bill_amount: 615.8
          ...55 fields
          ingestion_time: now()
        }
        "]
    end

    RAW --> T1 --> T2 --> T3 --> OUT
```

---

## LOB Routing Decision Tree

```mermaid
flowchart TD
    START["Incoming Record"] --> CHECK{"Extract lob field"}
    
    CHECK -->|lob = 'niineuat'| TAG1["OutputTag: niineuat"]
    CHECK -->|lob = 'simamyuat'| TAG2["OutputTag: simamyuat"]
    CHECK -->|lob = null/empty| UNKNOWN["OutputTag: unknown_lob"]
    CHECK -->|lob = 'other_value'| UNKNOWN
    
    TAG1 --> SINK1["Sink to<br/>ck_orders_niineuat"]
    TAG2 --> SINK2["Sink to<br/>ck_orders_simamyuat"]
    UNKNOWN --> LOG["⚠️ Log warning<br/>Skip record"]

    SINK1 --> S3_1[("S3: .../ck_orders_niineuat/")]
    SINK2 --> S3_2[("S3: .../ck_orders_simamyuat/")]

    style TAG1 fill:#c8e6c9
    style TAG2 fill:#c8e6c9
    style UNKNOWN fill:#ffcdd2
    style SINK1 fill:#81c784
    style SINK2 fill:#81c784
```

---

## File Structure & Responsibilities

```mermaid
flowchart TB
    subgraph Config["📁 Configuration"]
        YAML["application.yaml<br/><i>known-lobs list</i>"]
        IC["IcebergConfig.java<br/><i>Configuration POJO</i>"]
    end

    subgraph Source["📁 Kafka Source"]
        KSB["KafkaSourceBuilder.java<br/><i>Creates Kafka consumer</i>"]
    end

    subgraph Transform["📁 Transformer"]
        OHT["OrderHeaderTransformer.java<br/><i>Extracts root LOB</i><br/><i>Flattens 55 fields</i>"]
    end

    subgraph Sink["📁 Sink"]
        DLS["DynamicLobSink.java<br/><i>LOB routing logic</i><br/><i>Side outputs</i>"]
        ISB["IcebergSinkBuilder.java<br/><i>Static sink (legacy)</i>"]
    end

    subgraph Iceberg["📁 Iceberg"]
        IU["IcebergUtil.java<br/><i>TableLoader factory</i>"]
        CIT["CreateIcebergTables.java<br/><i>Schema definition</i><br/><i>Table creation</i>"]
    end

    subgraph Main["📁 Entry Point"]
        M["Main.java<br/><i>Flink job orchestration</i>"]
    end

    YAML --> IC
    M --> KSB
    M --> OHT
    M --> DLS
    DLS --> IU
    DLS --> CIT
    IC --> IU
    IC --> CIT

    style Config fill:#fff9c4
    style Source fill:#e1f5fe
    style Transform fill:#fff3e0
    style Sink fill:#e8f5e9
    style Iceberg fill:#f3e5f5
    style Main fill:#ffccbc
```

---

## Onboarding New LOB

```mermaid
sequenceDiagram
    participant Dev as Developer
    participant YAML as application.yaml
    participant Job as Flink Job
    participant DLS as DynamicLobSink
    participant CIT as CreateIcebergTables
    participant S3 as S3/Glue

    Dev->>YAML: Add "newlob" to known-lobs
    Dev->>Job: Restart Flink job
    Job->>DLS: Initialize with known LOBs
    DLS->>CIT: ensureTablesExist(["niineuat", "simamyuat", "newlob"])
    CIT->>S3: CREATE TABLE ck_orders_newlob (if not exists)
    S3-->>CIT: ✅ Table ready
    DLS->>DLS: Create OutputTag for "newlob"
    DLS->>DLS: Create FlinkSink for ck_orders_newlob
    Note over Job: Ready to process "newlob" data!
```

---

## Table Schema (56 Columns)

| Group | Fields | Count |
|-------|--------|-------|
| **Primary Key** | `id` | 1 |
| **Status** | `active_status`, `active_status_reason` | 2 |
| **Audit** | `created_by`, `creation_time`, `last_modified_time`, `modified_by`, `system_time` | 5 |
| **Business** | `lob`, `version`, `source` | 3 |
| **Amounts** | `bill_amount`, `net_amount`, `total_amount`, `total_initial_amt`, `total_mrp`, `nw`, `sales_value` | 7 |
| **Quantities** | `total_initial_quantity`, `total_quantity`, `normalized_quantity`, `initial_normalized_quantity`, `normalized_volume`, `line_count` | 6 |
| **Order IDs** | `order_number`, `reference_number`, `reference_order_number`, `remarks`, `ship_id` | 5 |
| **Location** | `location_hierarchy`, `outletcode`, `supplierid`, `hierarchy`, `user_hierarchy` | 5 |
| **GPS** | `gps_latitude`, `gps_longitude` | 2 |
| **Type/Status** | `type`, `sub_type`, `status`, `status_reason`, `processing_status`, `channel` | 6 |
| **Dates** | `delivery_date`, `sales_date` | 2 |
| **Beat** | `beat`, `beat_name`, `in_beat`, `in_range` | 4 |
| **Misc** | `group_id`, `loginid`, `hash`, `changed` | 4 |
| **JSON** | `extended_attributes`, `discount_info`, `order_details` | 3 |
| **Meta** | `ingestion_time` | 1 |
| | **Total** | **56** |
