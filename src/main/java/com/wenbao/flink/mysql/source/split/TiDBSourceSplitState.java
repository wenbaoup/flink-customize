package com.wenbao.flink.mysql.source.split;

public class TiDBSourceSplitState {
    
    private final TiDBSourceSplit split;
    
    public TiDBSourceSplitState(TiDBSourceSplit split) {
        this.split = split;
    }
    
    public TiDBSourceSplit toSplit() {
        return split;
    }
}
