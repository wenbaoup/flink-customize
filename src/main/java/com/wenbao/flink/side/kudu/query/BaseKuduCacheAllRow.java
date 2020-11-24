package com.wenbao.flink.side.kudu.query;

import com.wenbao.flink.side.all.BaseCacheAllRow;
import com.wenbao.flink.side.kudu.config.FlinkKuduConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class BaseKuduCacheAllRow<IN, OUT> extends BaseCacheAllRow<IN, OUT> {
    private static final long serialVersionUID = -178484556513273719L;
    private static final Logger LOG = LoggerFactory.getLogger(BaseKuduCacheAllRow.class);

    protected FlinkKuduConfig flinkKuduConfig;

    private KuduClient client;

    /**
     * 判断table是否已经打开连接
     */
    protected transient KuduTable table;
    /**
     * 查询查询的字段
     */
    protected List<String> queryFields;


    protected List<String> equalFieldList;


    public BaseKuduCacheAllRow(Long cacheTimeout) {
        super(cacheTimeout);
    }

    public BaseKuduCacheAllRow(Long cacheTimeout, FlinkKuduConfig flinkKuduConfig, List<String> queryFields, List<String> equalFieldList) {
        super(cacheTimeout);
        this.flinkKuduConfig = flinkKuduConfig;
        this.queryFields = queryFields;
        this.equalFieldList = equalFieldList;
    }

    protected void getAsyncKuduClientBuilder() {
        if (StringUtils.isBlank(flinkKuduConfig.getKuduMasters())) {
            throw new RuntimeException("Kudumasters cannot be empty");
        }
        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(flinkKuduConfig.getKuduMasters());
        if (null != flinkKuduConfig.getWorkerCount()) {
            kuduClientBuilder.workerCount(flinkKuduConfig.getWorkerCount());
        }
        if (null != flinkKuduConfig.getDefaultSocketReadTimeoutMs()) {
            kuduClientBuilder.defaultSocketReadTimeoutMs(flinkKuduConfig.getDefaultSocketReadTimeoutMs());
        }

        if (null != flinkKuduConfig.getDefaultOperationTimeoutMs()) {
            kuduClientBuilder.defaultOperationTimeoutMs(flinkKuduConfig.getDefaultOperationTimeoutMs());
        }
        client = kuduClientBuilder.build();
    }

    protected KuduScanner.KuduScannerBuilder getAsyncClient() throws KuduException {
        if (null == table) {
            getAsyncKuduClientBuilder();
            String tableName = flinkKuduConfig.getTableName();
            if (StringUtils.isBlank(tableName)) {
                throw new RuntimeException("tableName cannot be empty");
            }
            if (!client.tableExists(tableName)) {
                throw new IllegalArgumentException("Table Open Failed , please check table exists");
            }
            table = client.openTable(tableName);
            LOG.info("connect kudu is successed!");
        }
        KuduScanner.KuduScannerBuilder scannerBuilder = client.newScannerBuilder(table);
        Integer batchSizeBytes = flinkKuduConfig.getBatchSizeBytes();
        Long limitNum = flinkKuduConfig.getLimitNum();
        Boolean isFaultTolerant = flinkKuduConfig.getFaultTolerant();
        if (null != limitNum && limitNum >= 0) {
            scannerBuilder.limit(limitNum);
        }
        if (null != batchSizeBytes) {
            scannerBuilder.batchSizeBytes(batchSizeBytes);
        }
        if (null != isFaultTolerant) {
            scannerBuilder.setFaultTolerant(isFaultTolerant);
        }
        scannerBuilder.setProjectedColumnNames(queryFields);
        return scannerBuilder;
    }


}
