package com.wenbao.flink.mysql.source.base;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * TITableInfo
 *
 * @author wenbao
 * @since 2022/03/10
 */
public class TiTableInfo implements Serializable {
    
    private String name;
    private Map<String, TiColumnInfo> columnsMap;
    private List<TiColumnInfo> columns;
    private long rowSize;
    private TiColumnInfo primaryKeyColumn;
    
    
    public TiTableInfo() {
    }
    
    public TiTableInfo(Map<String, TiColumnInfo> columnsMap,
            List<TiColumnInfo> columns) {
        this.columnsMap = columnsMap;
        this.columns = columns;
    }
    
    
    public Map<String, TiColumnInfo> getColumnsMap() {
        return columnsMap;
    }
    
    public void setColumnsMap(
            Map<String, TiColumnInfo> columnsMap) {
        this.columnsMap = columnsMap;
    }
    
    public List<TiColumnInfo> getColumns() {
        return columns;
    }
    
    public void setColumns(List<TiColumnInfo> columns) {
        this.columns = columns;
    }
    
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public long getRowSize() {
        return rowSize;
    }
    
    public void setRowSize(long rowSize) {
        this.rowSize = rowSize;
    }
    
    public TiColumnInfo getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }
    
    public void setPrimaryKeyColumn(TiColumnInfo primaryKeyColumn) {
        this.primaryKeyColumn = primaryKeyColumn;
    }
}
