package com.wenbao.flink.mysql.source.row;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Row
 *
 * @author wenbao
 * @since 2022/03/10
 */
public interface Row extends Serializable {
    void setNull(int var1);
    
    boolean isNull(int var1);
    
    void setFloat(int var1, float var2);
    
    float getFloat(int var1);
    
    void setDouble(int var1, double var2);
    
    double getDouble(int var1);
    
    void setInteger(int var1, int var2);
    
    int getInteger(int var1);
    
    void setShort(int var1, short var2);
    
    short getShort(int var1);
    
    void setLong(int var1, long var2);
    
    long getLong(int var1);
    
    long getUnsignedLong(int var1);
    
    void setString(int var1, String var2);
    
    String getString(int var1);
    
    void setTime(int var1, Time var2);
    
    Date getTime(int var1);
    
    void setTimestamp(int var1, Timestamp var2);
    
    Timestamp getTimestamp(int var1);
    
    void setDate(int var1, Date var2);
    
    Date getDate(int var1);
    
    void setBytes(int var1, byte[] var2);
    
    byte[] getBytes(int var1);
    
    void set(int var1, String var2, Object var3);
    
    Object get(int var1, String var2);
    
    int fieldCount();
}
