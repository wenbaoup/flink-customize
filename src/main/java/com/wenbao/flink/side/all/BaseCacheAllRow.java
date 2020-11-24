package com.wenbao.flink.side.all;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class BaseCacheAllRow<IN, OUT> extends RichFlatMapFunction<IN, OUT> implements Serializable {
    private static final long serialVersionUID = 7697751897023353211L;
    private static final Logger LOG = LoggerFactory.getLogger(BaseCacheAllRow.class);

    protected Long cacheTimeout;

    public BaseCacheAllRow(Long cacheTimeout) {
        this.cacheTimeout = cacheTimeout;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Long start = System.currentTimeMillis();
        initCache();
        System.out.println("time" + (System.currentTimeMillis() - start));
        System.out.println("----- all cacheRef init end-----  ");
        //start reload cache thread
        ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("cache-all-reload"));
        es.scheduleAtFixedRate(this::reloadCache, cacheTimeout, cacheTimeout, TimeUnit.MILLISECONDS);
    }

    protected abstract void initCache() throws SQLException;

    protected abstract void reloadCache();

}
