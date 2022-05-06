package com.wenbao.flink.mysql.source.reader;

import com.wenbao.flink.mysql.source.split.TiDBSourceSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.table.data.RowData;

public class TiDBRecordEmitter implements RecordEmitter<RowData,
        RowData, TiDBSourceSplitState> {
    
    @Override
    public void emitRecord(RowData element,
            SourceOutput<RowData> sourceOutput, TiDBSourceSplitState state) {
        sourceOutput.collect(element);
    }
}
