package com.wenbao.flink.mysql.source.reader;

import com.wenbao.flink.mysql.source.split.TiDBSourceSplit;
import com.wenbao.flink.mysql.source.split.TiDBSourceSplitState;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.table.data.RowData;

import java.util.Map;
import java.util.function.Supplier;

public class TiDBSourceReader extends
        SingleThreadMultiplexSourceReaderBase<RowData, RowData,
                TiDBSourceSplit, TiDBSourceSplitState> {
    
    public TiDBSourceReader(
            Supplier<SplitReader<RowData, TiDBSourceSplit>> splitReaderSupplier,
            Configuration config,
            SourceReaderContext context) {
        super(splitReaderSupplier, new TiDBRecordEmitter(), config, context);
    }
    
    @Override
    protected void onSplitFinished(Map<String, TiDBSourceSplitState> map) {
    }
    
    @Override
    protected TiDBSourceSplitState initializedState(TiDBSourceSplit split) {
        return new TiDBSourceSplitState(split);
    }
    
    @Override
    protected TiDBSourceSplit toSplitType(String s, TiDBSourceSplitState state) {
        return state.toSplit();
    }
    
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
    }
    
    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
    }
}
