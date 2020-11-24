package com.wenbao.flink.side.kudu.config;

import java.io.Serializable;

public class FlinkKuduConfig implements Serializable {
    private static final long serialVersionUID = 95378257965058151L;

    private String kuduMasters;

    private String tableName;

    private Long defaultOperationTimeoutMs;

    private Long defaultSocketReadTimeoutMs;

    private Integer workerCount;

    /**
     * 查询返回的最大字节数
     */
    private Integer batchSizeBytes;

    /**
     * 查询返回数据条数
     */
    private Long limitNum;

    /**
     * 需要过滤的主键
     */
    private String primaryKey;
    /**
     * 过滤主键的最小值
     */
    private String lowerBoundPrimaryKey;
    /**
     * 过滤主键的最大值 不包含
     */
    private String upperBoundPrimaryKey;

    /**
     * 查询是否容错  查询失败是否扫描第二个副本  默认false  容错
     */
    private Boolean isFaultTolerant;


    public String getKuduMasters() {
        return kuduMasters;
    }

    public void setKuduMasters(String kuduMasters) {
        this.kuduMasters = kuduMasters;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Long getDefaultOperationTimeoutMs() {
        return defaultOperationTimeoutMs;
    }

    public void setDefaultOperationTimeoutMs(Long defaultOperationTimeoutMs) {
        this.defaultOperationTimeoutMs = defaultOperationTimeoutMs;
    }

    public Long getDefaultSocketReadTimeoutMs() {
        return defaultSocketReadTimeoutMs;
    }

    public void setDefaultSocketReadTimeoutMs(Long defaultSocketReadTimeoutMs) {
        this.defaultSocketReadTimeoutMs = defaultSocketReadTimeoutMs;
    }

    public Integer getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(Integer workerCount) {
        this.workerCount = workerCount;
    }

    public Integer getBatchSizeBytes() {
        return batchSizeBytes;
    }

    public void setBatchSizeBytes(Integer batchSizeBytes) {
        this.batchSizeBytes = batchSizeBytes;
    }

    public Long getLimitNum() {
        return limitNum;
    }

    public void setLimitNum(Long limitNum) {
        this.limitNum = limitNum;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getLowerBoundPrimaryKey() {
        return lowerBoundPrimaryKey;
    }

    public void setLowerBoundPrimaryKey(String lowerBoundPrimaryKey) {
        this.lowerBoundPrimaryKey = lowerBoundPrimaryKey;
    }

    public String getUpperBoundPrimaryKey() {
        return upperBoundPrimaryKey;
    }

    public void setUpperBoundPrimaryKey(String upperBoundPrimaryKey) {
        this.upperBoundPrimaryKey = upperBoundPrimaryKey;
    }

    public Boolean getFaultTolerant() {
        return isFaultTolerant;
    }

    public void setFaultTolerant(Boolean faultTolerant) {
        isFaultTolerant = faultTolerant;
    }
}
