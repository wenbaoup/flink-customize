package com.wenbao.flink.mysql.source.core;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class KeyRange {
    
    private String startKey;
    private String endKey;
    
    public KeyRange(String startKey, String endKey) {
        this.startKey = requireNonNull(startKey, "startKey is null");
        this.endKey = requireNonNull(endKey, "endKey is null");
    }
    
    public String getStartKey() {
        return startKey;
    }
    
    public String getEndKey() {
        return endKey;
    }
    
    public void setStartKey(String startKey) {
        this.startKey = startKey;
    }
    
    public void setEndKey(String endKey) {
        this.endKey = endKey;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(startKey, endKey);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        
        KeyRange other = (KeyRange) obj;
        return Objects.equals(this.startKey, other.startKey) && Objects
                .equals(this.endKey, other.endKey);
    }
    
    @Override
    public String toString() {
        return toStringHelper(this)
                .add("startKey", startKey)
                .add("endKey", endKey)
                .toString();
    }
}
