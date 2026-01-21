package com.salescode.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class IcebergConfig {

    private String warehouse;

    @JsonProperty("catalog-name")
    private String catalogName;

    private String database;
    private String table;

    // Glue Catalog specific fields
    @JsonProperty("catalog-type")
    private String catalogType = "glue";  // "glue" or "hadoop"

    @JsonProperty("io-impl")
    private String ioImpl = "org.apache.iceberg.aws.s3.S3FileIO";

    @JsonProperty("aws-region")
    private String awsRegion = "ap-south-1";
}
