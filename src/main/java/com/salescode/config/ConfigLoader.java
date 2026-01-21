package com.salescode.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;

public class ConfigLoader {

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    public static AppConfig loadConfig(String fileName) {
        try {
            InputStream input = ConfigLoader.class
                    .getClassLoader()
                    .getResourceAsStream(fileName);

            if (input == null) {
                throw new RuntimeException("Config file not found: " + fileName);
            }

            return mapper.readValue(input, AppConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config: " + fileName, e);
        }
    }
}
