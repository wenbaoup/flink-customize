package com.wenbao.flink.mysql.source.reader;

import com.wenbao.flink.mysql.source.TiDBSchemaAdapter;
import com.wenbao.flink.mysql.source.core.ClientSession;
import com.wenbao.flink.mysql.source.core.ColumnHandleInternal;
import com.wenbao.flink.mysql.source.split.TiDBSourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;

public class TiDBSourceSplitReader implements SplitReader<RowData, TiDBSourceSplit> {
    
    private final ClientSession session;
    private final List<ColumnHandleInternal> columns;
    private final TiDBSchemaAdapter schema;
    
    private List<TiDBSourceSplit> splits;
    private static final List<TiDBSourceSplit> EMPTY_SPLITS = new ArrayList<>(0);
    
    public TiDBSourceSplitReader(ClientSession session, List<ColumnHandleInternal> columns,
            TiDBSchemaAdapter schema) {
        this.session = session;
        this.columns = columns;
        this.schema = schema;
    }
    
    @Override
    public RecordsWithSplitIds<RowData> fetch() {
        try {
            return new TiDBSourceSplitRecords(session, splits, columns, schema);
        } finally {
            splits = EMPTY_SPLITS;
        }
    }
    
    @Override
    public void handleSplitsChanges(SplitsChange<TiDBSourceSplit> splitsChange) {
        // Get all the partition assignments and stopping offsets.
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }
        splits = splitsChange.splits();
    }
    
    @Override
    public void wakeUp() {
    }
    
    @Override
    public void close() throws Exception {
        session.close();
    }
}
