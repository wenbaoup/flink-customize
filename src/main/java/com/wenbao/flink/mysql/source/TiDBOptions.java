package com.wenbao.flink.mysql.source;

import com.google.common.collect.ImmutableSet;
import com.wenbao.flink.mysql.source.core.ClientConfig;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Set;

public class TiDBOptions {
    
    private static ConfigOption<String> required(String key) {
        return ConfigOptions.key(key).stringType().noDefaultValue();
    }
    
    private static ConfigOption<String> optional(String key, String value) {
        return ConfigOptions.key(key).stringType().defaultValue(value);
    }
    
    private static ConfigOption<String> optional(String key) {
        return optional(key, null);
    }
    
    public static final ConfigOption<String> DATABASE_URL = required(ClientConfig.DATABASE_URL);
    
    public static final ConfigOption<String> USERNAME = required(ClientConfig.USERNAME);
    
    public static final ConfigOption<String> PASSWORD = required(ClientConfig.PASSWORD);
    
    public static final ConfigOption<String> DATABASE_NAME = required("tidb.database.name");
    
    public static final ConfigOption<String> TABLE_NAME = required("tidb.table.name");
    
    public static final ConfigOption<String> MAX_POOL_SIZE = required(ClientConfig.MAX_POOL_SIZE);
    
    public static final ConfigOption<String> MIN_IDLE_SIZE = required(ClientConfig.MIN_IDLE_SIZE);
    
    
    
    public static final ConfigOption<String> STREAMING_SOURCE = optional("tidb.streaming.source");
    
    public static final String STREAMING_SOURCE_KAFKA = "kafka";
    
    public static final Set<String> VALID_STREAMING_SOURCES = ImmutableSet.of(
            STREAMING_SOURCE_KAFKA);
    
    public static final ConfigOption<String> STREAMING_CODEC = optional("tidb.streaming.codec");
    
    public static final String STREAMING_CODEC_JSON = "json";
    public static final String STREAMING_CODEC_CANAL_JSON = "canal-json";
    public static final Set<String> VALID_STREAMING_CODECS =
            ImmutableSet.of(STREAMING_CODEC_CANAL_JSON, STREAMING_CODEC_JSON);
    
    public static final String TIDB_KAFKA_TABLE_TYPE_ZIPPER = "zipper";
    
    
    
    public static Set<ConfigOption<?>> requiredOptions() {
        return withMoreRequiredOptions();
    }
    
    public static Set<ConfigOption<?>> withMoreRequiredOptions(ConfigOption<?>... options) {
        return ImmutableSet.<ConfigOption<?>>builder()
                .add(DATABASE_URL, DATABASE_NAME, TABLE_NAME, USERNAME)
                .add(options)
                .build();
    }
    
    public static Set<ConfigOption<?>> optionalOptions() {
        return withMoreOptionalOptions();
    }
    
    public static Set<ConfigOption<?>> withMoreOptionalOptions(ConfigOption<?>... options) {
        return ImmutableSet.<ConfigOption<?>>builder().add(
                PASSWORD,
                MAX_POOL_SIZE,
                MIN_IDLE_SIZE,
                STREAMING_SOURCE)
                .add(options)
                .build();
    }
}
