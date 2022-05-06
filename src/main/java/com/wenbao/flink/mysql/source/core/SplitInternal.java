package com.wenbao.flink.mysql.source.core;

import java.io.Serializable;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class SplitInternal implements Serializable {
    
    private final TableHandleInternal table;
    private final String startKey;
    private final String endKey;
    
    public SplitInternal(
            TableHandleInternal table,
            String startKey,
            String endKey) {
        this.table = requireNonNull(table, "table is null");
        this.startKey = requireNonNull(startKey, "startKey is null");
        this.endKey = requireNonNull(endKey, "endKey is null");
    }
    
    public SplitInternal(
            TableHandleInternal table,
            KeyRange range) {
        this(table, range.getStartKey(), range.getEndKey());
    }
    
    public TableHandleInternal getTable() {
        return table;
    }
    
    public String getStartKey() {
        return startKey;
    }
    
    public String getEndKey() {
        return endKey;
    }
    
    
    @Override
    public int hashCode() {
        return Objects.hash(table, startKey, endKey);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        
        SplitInternal other = (SplitInternal) obj;
        return Objects.equals(this.table, other.table)
                && Objects.equals(this.startKey, other.startKey)
                && Objects.equals(this.endKey, other.endKey);
    }
    
    @Override
    public String toString() {
        return toStringHelper(this)
                .add("table", table)
                .add("startKey", startKey)
                .add("endKey", endKey)
                .toString();
    }
}