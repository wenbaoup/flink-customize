package com.wenbao.flink.mysql.source.cdc;

/**
 * MysqlDmlType
 *
 * @author wenbao
 * @since 2022/03/16
 */
public class MysqlCDCConstants {
    
    public static final String INSERT = "INSERT";
    
    public static final String UPDATE = "UPDATE";
    
    public static final String DELETE = "DELETE";
    
    public static final String UPDATE_BEFORE = "update_before";
    
    public static final String UPDATE_AFTER = "update_after";
    
    public static final String MATE_TIDB_PREFIX = "mate_tidb";
    
    public static final String MATE_TIDB_TYPE = "mate_tidb_type";
    
    public static final String MATE_TIDB_TS = "mate_tidb_ts";
    
    public static final String MATE_TIDB_ES = "mate_tidb_es";
    
    
}