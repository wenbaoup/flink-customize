package com.wenbao.flink.mysql.source;

import com.alibaba.fastjson.JSONObject;
import com.wenbao.flink.mysql.source.cdc.CanalJsonEvent;
import com.wenbao.flink.mysql.source.enumerator.TiDBSourceSplitEnumerator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.wenbao.flink.mysql.source.TiDBOptions.*;
import static com.wenbao.flink.mysql.source.cdc.CDCOptions.IGNORE_PARSE_ERRORS;
import static com.wenbao.flink.mysql.source.cdc.MysqlCDCConstants.*;
import static com.wenbao.flink.mysql.source.core.TIDBConfigConstants.*;

public class TiDBSourceBuilder implements Serializable {
    
    private final String VALUE_TRUE = "true";
    
    private String databaseName;
    private String tableName;
    private String streamingSource;
    private String streamingCodec;
    private Map<String, String> properties;
    private boolean ignoreParseErrors;
    private final TiDBSchemaAdapter schema;
    
    public TiDBSourceBuilder(ResolvedCatalogTable table,
            Function<DataType, TypeInformation<RowData>> typeInfoFactory, int[] projectedFields) {
        schema = new TiDBSchemaAdapter(table, typeInfoFactory, projectedFields);
        setProperties(table.getOptions());
    }
    
    private static String validateRequired(String key, String value) {
        Preconditions.checkNotNull(value, "'%s' is not set", key);
        Preconditions.checkArgument(!value.trim().isEmpty(),
                "'%s' is not set", key);
        return value;
    }
    
    private static String validateProperty(String key, String value, Set<String> validOptions) {
        if (!validOptions.contains(value)) {
            throw new IllegalArgumentException("Invalid value '" + value + "' for '" + key + "'");
        }
        return value;
    }
    
    private String getRequiredProperty(String key) {
        return validateRequired(key, properties.get(key));
    }
    
    private Optional<String> getOptionalProperty(String key) {
        return Optional.ofNullable(properties.get(key));
    }
    
    private TiDBSourceBuilder setProperties(Map<String, String> properties) {
        this.properties = properties;
        this.databaseName = getRequiredProperty(DATABASE_NAME.key());
        this.tableName = getRequiredProperty(TABLE_NAME.key());
        this.streamingSource = getOptionalProperty(STREAMING_SOURCE.key())
                .map(v -> validateProperty(STREAMING_SOURCE.key(), v, VALID_STREAMING_SOURCES))
                .orElse(null);
        this.streamingCodec = getOptionalProperty(STREAMING_CODEC.key())
                .map(v -> validateProperty(STREAMING_CODEC.key(), v, VALID_STREAMING_CODECS))
                .orElse(STREAMING_CODEC_CANAL_JSON);
        this.ignoreParseErrors = getOptionalProperty(IGNORE_PARSE_ERRORS.key())
                .map(Boolean::parseBoolean).orElse(false);
        return this;
    }
    
    
    public Source<RowData, ?, ?> build() {
        if (null != properties.get(TIDB_SOURCE_ONLY_KAFKA) &&
                VALUE_TRUE.equals(properties.get(TIDB_SOURCE_ONLY_KAFKA))) {
            return getRowDataKafkaSource();
        }
        final SnapshotSource source = new SnapshotSource(databaseName, tableName, properties,
                schema);
        if (streamingSource == null) {
            return source;
        }
        HybridSource.HybridSourceBuilder<RowData, TiDBSourceSplitEnumerator> builder =
                HybridSource.builder(source);
        return builder.addSource(getRowDataKafkaSource()).build();
    }
    
    private KafkaSource<RowData> getRowDataKafkaSource() {
        return KafkaSource.<RowData>builder()
                .setDeserializer(new KafkaRecordDeserializationSchema<RowData>() {
                    @Override
                    public TypeInformation<RowData> getProducedType() {
                        return schema.getProducedType();
                    }
                    
                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord,
                            Collector<RowData> collector) {
                        String value = new String(consumerRecord.value());
                        if (streamingCodec.equals(STREAMING_CODEC_CANAL_JSON)) {
                            CanalJsonEvent canalJsonEvent = JSONObject.parseObject(value,
                                    CanalJsonEvent.class);
                            String data = canalJsonEvent.getData();
                            String old = canalJsonEvent.getOld();
                            if (null != properties.get(TIDB_KAFKA_ONLY_INSERT)) {
                                //如果只发送insert数据 源头不应该产生delete操作 源头可以有delete数据但是数仓不会删除数据 只会加isDelete标识
                                cdcSendInsertData(collector, canalJsonEvent, data, old);
                                return;
                            }
                            cdcSendData(collector, canalJsonEvent, data, old);
                        } else {
                            collector.collect(schema.kafkaDataConvert(
                                    JSONObject.parseObject(value, Map.class),
                                    INSERT, null));
                        }
                    }
                })
                .setBootstrapServers(properties.get(TIDB_KAFKA_BOOTSTRAP))
                .setStartingOffsets(OffsetsInitializer.timestamp(
                        Long.parseLong(properties.get(TIDB_KAFKA_READ_DATA_TIME))))
                .setTopics(properties.get(TIDB_KAFKA_TOPIC))
                .setGroupId(properties.get(TIDB_KAFKA_GROUP))
                
                .build();
    }
    
    private void cdcSendInsertData(Collector<RowData> collector, CanalJsonEvent canalJsonEvent,
            String data, String old) {
        switch (canalJsonEvent.getType()) {
            case INSERT:
            case UPDATE:
                collector.collect(
                        schema.kafkaDataConvert(
                                JSONObject.parseObject(JSONObject.parseArray(
                                        data, String.class).get(0), Map.class),
                                INSERT, canalJsonEvent));
                break;
            case DELETE:
                sendDeleteData(collector, canalJsonEvent, data, old, INSERT);
                break;
        }
    }
    
    private void cdcSendData(Collector<RowData> collector, CanalJsonEvent canalJsonEvent,
            String data, String old) {
        switch (canalJsonEvent.getType()) {
            case INSERT:
                collector.collect(
                        schema.kafkaDataConvert(
                                JSONObject.parseObject(JSONObject.parseArray(
                                        data, String.class).get(0),
                                        Map.class),
                                INSERT, canalJsonEvent));
                break;
            case UPDATE:
                if (TIDB_KAFKA_TABLE_TYPE_ZIPPER.equals(
                        properties.get(TIDB_KAFKA_TABLE_TYPE))) {
                    //判断是否是关键字段修改了 废弃  如果只判断关键字会导致updateTime对不齐上一个有效的
//                    String columns = properties.get(TIDB_KAFKA_TABLE_ZIPPER_COLUMNS);
//                    if (StringUtils.isBlank(columns)) {
//                        throw new RuntimeException(
//                                "tidb.kafka.table.type is zipper ,tidb.kafka.table.zipper.columns value can't be empty ");
//                    }
//                    Map oldMap = JSONObject.parseObject(JSONObject.parseArray(
//                            old, String.class).get(0), Map.class);
//                    Map dataMap = JSONObject.parseObject(JSONObject.parseArray(
//                            data, String.class).get(0), Map.class);
//                    String[] split = columns.split(",");
//                    boolean flag = false;
//                    for (String column : split) {
//                        String dataValue = null == dataMap.get(column) ? ""
//                                : String.valueOf(dataMap.get(column));
//                        String oldValue = null == oldMap.get(column) ? ""
//                                : String.valueOf(oldMap.get(column));
//                        if (!dataValue.equals(oldValue)) {
//                            flag = true;
//                            break;
//                        }
//                    }
                    //将原有update操作转换为insert+delete
//                    if (flag) {
//                        collector.collect(
//                                schema.kafkaDataConvert(
//                                        JSONObject.parseObject(JSONObject.parseArray(
//                                                old, String.class).get(0), Map.class),
//                                        DELETE, new CanalJsonEvent(DELETE, canalJsonEvent.getEs(),
//                                                canalJsonEvent.getTs(), canalJsonEvent.getData(),
//                                                canalJsonEvent.getOld())));
//                        collector.collect(
//                                schema.kafkaDataConvert(
//                                        JSONObject.parseObject(JSONObject.parseArray(
//                                                data, String.class).get(0), Map.class),
//                                        INSERT, new CanalJsonEvent(INSERT, canalJsonEvent.getEs(),
//                                                canalJsonEvent.getTs(), canalJsonEvent.getData(),
//                                                canalJsonEvent.getOld())));
//                    }
                    collector.collect(
                            schema.kafkaDataConvert(
                                    JSONObject.parseObject(JSONObject.parseArray(
                                            old, String.class).get(0), Map.class),
                                    //不能发送类型为delete的数据
                                    //delete类型数据在使用'connector' = 'upsert-kafka' 时会导致发送上kafka的value都是null的情况
                                    INSERT, new CanalJsonEvent(DELETE, canalJsonEvent.getEs(),
                                            canalJsonEvent.getTs(), canalJsonEvent.getData(),
                                            canalJsonEvent.getOld())));
                    collector.collect(
                            schema.kafkaDataConvert(
                                    JSONObject.parseObject(JSONObject.parseArray(
                                            data, String.class).get(0), Map.class),
                                    INSERT, new CanalJsonEvent(INSERT, canalJsonEvent.getEs(),
                                            canalJsonEvent.getTs(), canalJsonEvent.getData(),
                                            canalJsonEvent.getOld())));
                } else {
                    collector.collect(
                            schema.kafkaDataConvert(
                                    JSONObject.parseObject(JSONObject.parseArray(
                                            old, String.class).get(0),
                                            Map.class),
                                    UPDATE_BEFORE, canalJsonEvent));
                    collector.collect(
                            schema.kafkaDataConvert(
                                    JSONObject.parseObject(JSONObject.parseArray(
                                            data, String.class).get(0),
                                            Map.class),
                                    UPDATE_AFTER, canalJsonEvent));
                }
                break;
            case DELETE:
                if (TIDB_KAFKA_TABLE_TYPE_ZIPPER.equals(
                        properties.get(TIDB_KAFKA_TABLE_TYPE))) {
                    sendDeleteData(collector, canalJsonEvent, data, old, INSERT);
                } else {
                    sendDeleteData(collector, canalJsonEvent, data, old, DELETE);
                }
            
        }
    }
    
    private void sendDeleteData(Collector<RowData> collector, CanalJsonEvent canalJsonEvent,
            String data,
            String old, String insert) {
        // 兼容delete数据在old中的情况
        if (data != null) {
            collector.collect(
                    schema.kafkaDataConvert(
                            JSONObject.parseObject(JSONObject.parseArray(
                                    data, String.class).get(0), Map.class),
                            insert, canalJsonEvent));
        } else {
            collector.collect(
                    schema.kafkaDataConvert(
                            JSONObject.parseObject(JSONObject.parseArray(
                                    old, String.class).get(0), Map.class),
                            insert, canalJsonEvent));
        }
    }
}
