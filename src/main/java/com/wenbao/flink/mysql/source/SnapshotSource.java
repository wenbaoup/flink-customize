package com.wenbao.flink.mysql.source;

import com.wenbao.flink.mysql.source.core.ClientConfig;
import com.wenbao.flink.mysql.source.core.ClientSession;
import com.wenbao.flink.mysql.source.core.ColumnHandleInternal;
import com.wenbao.flink.mysql.source.enumerator.TiDBSourceSplitEnumState;
import com.wenbao.flink.mysql.source.enumerator.TiDBSourceSplitEnumStateSerializer;
import com.wenbao.flink.mysql.source.enumerator.TiDBSourceSplitEnumerator;
import com.wenbao.flink.mysql.source.reader.TiDBSourceReader;
import com.wenbao.flink.mysql.source.reader.TiDBSourceSplitReader;
import com.wenbao.flink.mysql.source.split.TiDBSourceSplit;
import com.wenbao.flink.mysql.source.split.TiDBSourceSplitSerializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

import java.util.List;
import java.util.Map;

public class SnapshotSource implements Source<RowData, TiDBSourceSplit, TiDBSourceSplitEnumState>,
        ResultTypeQueryable<RowData> {
    
    private final String databaseName;
    private final String tableName;
    private final Map<String, String> properties;
    private final TiDBSchemaAdapter schema;
    
    public SnapshotSource(String databaseName, String tableName,
            Map<String, String> properties, TiDBSchemaAdapter schema) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.properties = properties;
        this.schema = schema;
    }
    
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }
    
    private static Configuration toConfiguration(Map<String, String> properties) {
        Configuration config = new Configuration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            config.setString(entry.getKey(), entry.getValue());
        }
        return config;
    }
    
    @Override
    public SourceReader<RowData, TiDBSourceSplit>
    createReader(SourceReaderContext context) throws Exception {
        ClientSession session = null;
        try {
            final Map<String, String> properties = this.properties;
            session = ClientSession.createWithSingleConnection(new ClientConfig(properties));
            final List<ColumnHandleInternal> columns =
                    session.getTableColumns(databaseName, tableName, schema.getPhysicalFieldNames())
                            .orElseThrow(() -> new NullPointerException(
                                    "Could not get columns for TiDB table:"
                                            + databaseName + "." + tableName));
            final ClientSession s = session;
//            schema.open();
            return new TiDBSourceReader(
                    () -> new TiDBSourceSplitReader(s, columns, schema),
                    toConfiguration(properties), context);
        } catch (Exception ex) {
            if (session != null) {
                session.close();
            }
            throw ex;
        }
    }
    
    @Override
    public SplitEnumerator<TiDBSourceSplit, TiDBSourceSplitEnumState> createEnumerator(
            SplitEnumeratorContext<TiDBSourceSplit> context) {
        return new TiDBSourceSplitEnumerator(this.properties, context);
    }
    
    @Override
    public SplitEnumerator<TiDBSourceSplit, TiDBSourceSplitEnumState> restoreEnumerator(
            SplitEnumeratorContext<TiDBSourceSplit> context,
            TiDBSourceSplitEnumState state) {
        return new TiDBSourceSplitEnumerator(this.properties, context, state.assignedSplits());
    }
    
    @Override
    public SimpleVersionedSerializer<TiDBSourceSplit> getSplitSerializer() {
        return new TiDBSourceSplitSerializer();
    }
    
    @Override
    public SimpleVersionedSerializer<TiDBSourceSplitEnumState> getEnumeratorCheckpointSerializer() {
        return new TiDBSourceSplitEnumStateSerializer();
    }
    
    @Override
    public TypeInformation<RowData> getProducedType() {
        return schema.getProducedType();
    }
}