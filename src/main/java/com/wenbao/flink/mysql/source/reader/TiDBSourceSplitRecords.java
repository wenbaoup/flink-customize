package com.wenbao.flink.mysql.source.reader;

import com.wenbao.flink.mysql.source.TiDBSchemaAdapter;
import com.wenbao.flink.mysql.source.core.*;
import com.wenbao.flink.mysql.source.split.TiDBSourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TiDBSourceSplitRecords implements RecordsWithSplitIds<RowData> {
    
    private final Set<String> finishedSplits;
    private final TiDBSourceSplit[] splits;
    private int nextSplit;
    private final ClientSession session;
    private RecordCursorInternal cursor;
    private final List<ColumnHandleInternal> columns;
    private final TiDBSchemaAdapter schema;
    
    public TiDBSourceSplitRecords(ClientSession session, List<TiDBSourceSplit> splits,
            List<ColumnHandleInternal> columns, TiDBSchemaAdapter schema) {
        this.session = session;
        this.splits = splits.toArray(new TiDBSourceSplit[0]);
        this.finishedSplits = splits.stream()
                .map(TiDBSourceSplit::splitId)
                .collect(Collectors.toSet());
        this.schema = schema;
        this.columns = columns;
    }
    
    @Nullable
    @Override
    public String nextSplit() {
        if (nextSplit >= splits.length) {
            return null;
        }
        int currentSplit = nextSplit;
        nextSplit = currentSplit + 1;
        TiDBSourceSplit split = splits[currentSplit];
        SplitInternal splitInternal = split.getSplit();
        RecordSetInternal recordSetInternal = new RecordSetInternal(session,
                splitInternal, columns, Optional.empty());
        cursor = recordSetInternal.cursor();
        return splits[currentSplit].splitId();
    }
    
    @Nullable
    @Override
    public RowData nextRecordFromSplit() {
        if (!cursor.advanceNextPosition()) {
            return null;
        }
        return schema.convert(cursor);
    }
    
    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }
}