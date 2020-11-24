package com.wenbao.flink.side.kudu.query;

import com.wenbao.flink.side.kudu.config.FlinkKuduConfig;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public abstract class BaseAsyncKuduRow<IN, OUT> extends RichAsyncFunction<IN, OUT> implements Serializable {
    private static final long serialVersionUID = 7697751897023353211L;
    private static final Logger LOG = LoggerFactory.getLogger(BaseAsyncKuduRow.class);

    protected FlinkKuduConfig flinkKuduConfig;

    protected transient AsyncKuduClient asyncClient;

    /**
     * 判断table是否已经打开连接
     */
    protected transient KuduTable table;
    /**
     * 查询查询的字段
     */
    protected List<String> queryFields;

    public BaseAsyncKuduRow(FlinkKuduConfig flinkKuduConfig, List<String> queryFields) {
        this.flinkKuduConfig = flinkKuduConfig;
        this.queryFields = queryFields;
    }

    protected BaseAsyncKuduRow() {
    }

    @Override
    public void close() throws IOException {
        if (null != asyncClient) {
            try {
                super.close();
                asyncClient.close();
            } catch (Exception e) {
                LOG.error("Error while closing client.", e);
            }
        }
    }

}
