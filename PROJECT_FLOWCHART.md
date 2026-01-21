flowchart LR
%% =======================
%% External Systems
%% =======================
subgraph EXT["🌍 External Systems"]
KAFKA["☁️ Apache Kafka<br/>entity-change-events"]
MINIO["💾 MinIO (S3)<br/>Object Storage"]
end

    %% =======================
    %% Flink Application
    %% =======================
    subgraph FLINK["⚡ Apache Flink CDC Application"]
        MAIN["Main.java<br/>🚀 Job Entry Point"]

        %% =======================
        %% Startup / Bootstrap
        %% =======================
        subgraph BOOT["🟢 Application Bootstrap"]
            YAML["📄 application.yaml"]
            CL["ConfigLoader.loadConfig()"]
            AC["AppConfig"]
            ENV["StreamExecutionEnvironment<br/>getExecutionEnvironment()"]
            PAR["setParallelism(1)"]
            INIT["IcebergTableInitializer<br/>ensureTablesExist()"]
        end

        %% =======================
        %% Source
        %% =======================
        subgraph SRC["📥 Source Layer"]
            KSB["KafkaSourceBuilder"]
            KS["KafkaSource<ObjectNode>"]
        end

        %% =======================
        %% Transform
        %% =======================
        subgraph TX["🔄 Transformation Layer"]
            OHT["OrderHeaderTransformer"]
            ODT["OrderDetailsTransformer"]
        end

        %% =======================
        %% Sink
        %% =======================
        subgraph SNK["📤 Sink Layer"]
            IU["IcebergUtil"]
            ISB["IcebergSinkBuilder"]
            HM["OrderHeaderMapper<br/>→ RowData"]
            DM["OrderDetailsMapper<br/>→ RowData"]
        end
    end

    %% =======================
    %% Iceberg Tables
    %% =======================
    subgraph ICE["🧊 Iceberg Tables"]
        ORD["📊 db.orders<br/>Order Headers"]
        ODTBL["📊 db.order_details<br/>Order Line Items"]
    end

    %% =======================
    %% Bootstrap Flow
    %% =======================
    MAIN --> YAML --> CL --> AC
    MAIN --> ENV --> PAR
    PAR --> INIT

    %% =======================
    %% Streaming Flow
    %% =======================
    KAFKA --> KSB --> KS
    KS --> OHT
    KS --> ODT

    OHT --> HM --> ORD
    ODT --> DM --> ODTBL

    ORD --> MINIO
    ODTBL --> MINIO

    %% =======================
    %% Wiring
    %% =======================
    AC --> KSB
    AC --> ISB
    ISB --> IU
