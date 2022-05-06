package com.wenbao.flink.mysql.source.core;

import com.wenbao.flink.mysql.source.iterator.CoprocessorIterator;
import com.wenbao.flink.mysql.source.row.Row;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class RecordCursorInternal {
    
    private final List<ColumnHandleInternal> columnHandles;
    private final CoprocessorIterator<Row> iterator;
    private Row row = null;
    
    public RecordCursorInternal(List<ColumnHandleInternal> columnHandles,
            CoprocessorIterator<Row> iterator) {
        this.columnHandles = columnHandles;
        this.iterator = iterator;
    }
    
    public String getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }
    
    public boolean advanceNextPosition() {
        if (iterator.hasNext()) {
            row = iterator.next();
            return true;
        } else {
            return false;
        }
    }
    
    public void close() {
    }
    
    public Object getObject(int field) {
        return row.get(field, null);
    }
    
    public BigDecimal getBigDecimal(int field) {
        return (BigDecimal) row.get(field, null);
    }
    
    public boolean isNull(int field) {
        return row.isNull(field);
    }
    
    public float getFloat(int field) {
        return (float) row.getDouble(field);
    }
    
    public boolean getBoolean(int field) {
        return (boolean) row.get(field, null);
    }
    
    public byte getByte(int field) {
        return (byte) row.getLong(field);
    }
    
    public double getDouble(int field) {
        return row.getDouble(field);
    }
    
    public int getInteger(int field) {
        return (int) row.getLong(field);
    }
    
    public short getShort(int field) {
        return (short) row.getLong(field);
    }
    
    public long getLong(int field) {
        return row.getLong(field);
    }
    
    public long getUnsignedLong(int field) {
        return row.getUnsignedLong(field);
    }
    
    public String getString(int field) {
        return row.getString(field);
    }
    
    public Time getTime(int field) {
        return new Time(row.getLong(field) / 1000000);
    }
    
    public Timestamp getTimestamp(int field) {
        return row.getTimestamp(field);
    }
    
    public Date getDate(int field) {
        return row.getDate(field);
    }
    
    public byte[] getBytes(int field) {
        return row.getBytes(field);
    }
    
    public int fieldCount() {
        return row.fieldCount();
    }
    
    @Override
    public String toString() {
        return toStringHelper(this)
                .add("columns", columnHandles)
                .toString();
    }
}
