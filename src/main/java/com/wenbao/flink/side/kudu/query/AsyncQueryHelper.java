package com.wenbao.flink.side.kudu.query;

import com.wenbao.flink.side.kudu.config.FlinkKuduConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AsyncQueryHelper {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncQueryHelper.class);

    public static AsyncKuduClient getAsyncKuduClientBuilder(FlinkKuduConfig flinkKuduConfig) {
        if (StringUtils.isBlank(flinkKuduConfig.getKuduMasters())) {
            throw new RuntimeException("Kudumasters cannot be empty");
        }
        AsyncKuduClient.AsyncKuduClientBuilder asyncKuduClientBuilder = new AsyncKuduClient.AsyncKuduClientBuilder(flinkKuduConfig.getKuduMasters());
        if (null != flinkKuduConfig.getWorkerCount()) {
            asyncKuduClientBuilder.workerCount(flinkKuduConfig.getWorkerCount());
        }
        if (null != flinkKuduConfig.getDefaultSocketReadTimeoutMs()) {
            asyncKuduClientBuilder.defaultSocketReadTimeoutMs(flinkKuduConfig.getDefaultSocketReadTimeoutMs());
        }

        if (null != flinkKuduConfig.getDefaultOperationTimeoutMs()) {
            asyncKuduClientBuilder.defaultOperationTimeoutMs(flinkKuduConfig.getDefaultOperationTimeoutMs());
        }
        return asyncKuduClientBuilder.build();
    }

    public static KuduTable getKuduTable(FlinkKuduConfig flinkKuduConfig) {
        try {
            AsyncKuduClient asyncClient = getAsyncKuduClientBuilder(flinkKuduConfig);
            String tableName = flinkKuduConfig.getTableName();
            if (StringUtils.isBlank(tableName)) {
                throw new RuntimeException("tableName cannot be empty");
            }
            if (!asyncClient.syncClient().tableExists(tableName)) {
                throw new IllegalArgumentException("Table Open Failed , please check table exists");
            }
            LOG.info("connect kudu is successed!");
            return asyncClient.syncClient().openTable(tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static AsyncKuduScanner.AsyncKuduScannerBuilder getAsyncClient(FlinkKuduConfig flinkKuduConfig, AsyncKuduClient asyncClient, List<String> queryFields, KuduTable table)
            throws KuduException {
        AsyncKuduScanner.AsyncKuduScannerBuilder scannerBuilder = asyncClient.newScannerBuilder(table);
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
