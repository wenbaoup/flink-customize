package com.wenbao.flink.mysql.source.row;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * ObjectRowImpl
 *
 * @author wenbao
 * @since 2022/03/10
 */
public class ObjectRowImpl implements Row {
    
    private final Object[] values;
    
    private ObjectRowImpl(Object[] values) {
        this.values = values;
    }
    
    private ObjectRowImpl(int fieldCount) {
        this.values = new Object[fieldCount];
    }
    
    public static Row create(Object[] values) {
        return new ObjectRowImpl(values);
    }
    
    public static Row create(int fieldCount) {
        return new ObjectRowImpl(fieldCount);
    }
    
    public void setNull(int pos) {
        this.values[pos] = null;
    }
    
    public boolean isNull(int pos) {
        return this.values[pos] == null;
    }
    
    public void setFloat(int pos, float v) {
        this.values[pos] = v;
    }
    
    public float getFloat(int pos) {
        return (Float) this.values[pos];
    }
    
    public void setInteger(int pos, int v) {
        this.values[pos] = v;
    }
    
    public int getInteger(int pos) {
        return (Integer) this.values[pos];
    }
    
    public void setShort(int pos, short v) {
        this.values[pos] = v;
    }
    
    public short getShort(int pos) {
        return (Short) this.values[pos];
    }
    
    public void setDouble(int pos, double v) {
        this.values[pos] = v;
    }
    
    public double getDouble(int pos) {
        return (Double) this.values[pos];
    }
    
    public void setLong(int pos, long v) {
        this.values[pos] = v;
    }
    
    public long getLong(int pos) {
        return (Long) this.values[pos];
    }
    
    public long getUnsignedLong(int pos) {
        return ((BigDecimal) this.values[pos]).longValue();
    }
    
    public void setString(int pos, String v) {
        this.values[pos] = v;
    }
    
    public String getString(int pos) {
        return String.valueOf(this.values[pos]);
    }
    
    public void setTime(int pos, Time v) {
        this.values[pos] = v;
    }
    
    public Date getTime(int pos) {
        return (Date) this.values[pos];
    }
    
    public void setTimestamp(int pos, Timestamp v) {
        this.values[pos] = v;
    }
    
    public Timestamp getTimestamp(int pos) {
        return (Timestamp) this.values[pos];
    }
    
    public void setDate(int pos, Date v) {
        this.values[pos] = v;
    }
    
    public Date getDate(int pos) {
        return (Date) this.values[pos];
    }
    
    public void setBytes(int pos, byte[] v) {
        this.values[pos] = v;
    }
    
    public byte[] getBytes(int pos) {
        return (byte[]) ((byte[]) this.values[pos]);
    }
    
    public void set(int pos, String type, Object v) {
        this.values[pos] = v;
    }
    
    public Object get(int pos, String type) {
        return this.values[pos];
    }
    
    public int fieldCount() {
        return this.values.length;
    }
    
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("(");
        
        for (int i = 0; i < this.values.length; ++i) {
            if (this.values[i] instanceof byte[]) {
                builder.append("[");
                builder.append(formatBytes((byte[]) ((byte[]) this.values[i])));
                builder.append("]");
            } else if (this.values[i] instanceof BigDecimal) {
                builder.append(((BigDecimal) this.values[i]).toPlainString());
            } else {
                builder.append(this.values[i]);
            }
            
            if (i < this.values.length - 1) {
                builder.append(",");
            }
        }
        
        builder.append(")");
        return builder.toString();
    }
    
    public static String formatBytes(byte[] bytes) {
        if (bytes == null) {
            return "null";
        } else {
            StringBuilder sb = new StringBuilder();
            
            for (int i = 0; i < bytes.length; ++i) {
                int unsignedByte = toInt(bytes[i]);
                sb.append(unsignedByte);
                if (i != bytes.length - 1) {
                    sb.append(",");
                }
            }
            
            return sb.toString();
        }
    }
    
    public static int toInt(byte value) {
        return value & 255;
    }
}