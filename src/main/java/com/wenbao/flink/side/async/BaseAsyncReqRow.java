/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.wenbao.flink.side.async;

import com.wenbao.flink.side.cache.AbsSideCache;
import com.wenbao.flink.side.cache.CacheObj;
import com.wenbao.flink.side.cache.LRUSideCache;
import com.wenbao.flink.side.config.LRUCacheConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.async.queue.StreamRecordQueueEntry;

import java.util.concurrent.TimeoutException;


public abstract class BaseAsyncReqRow<IN, OUT> extends RichAsyncFunction<IN, OUT> {

    private static final long serialVersionUID = 2098635244857937717L;

    protected LRUCacheConfig lruCacheConfig;

    private AbsSideCache sideCache;

    public BaseAsyncReqRow(LRUCacheConfig lruCacheConfig) {
        this.lruCacheConfig = lruCacheConfig;
    }

    public BaseAsyncReqRow() {
    }

    @Override
    public void timeout(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        StreamRecordQueueEntry<OUT> future = (StreamRecordQueueEntry<OUT>) resultFuture;
        try {
            if (null == future.get()) {
                throw new TimeoutException("Async function call has timed out.");
            }
        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    protected void initCache() {
        sideCache = new LRUSideCache(lruCacheConfig);
        sideCache.initCache();
    }

    protected CacheObj getFromCache(String key) {
        return sideCache.getFromCache(key);
    }

    protected void putCache(String key, CacheObj value) {
        sideCache.putCache(key, value);
    }

    protected boolean openCache() {
        return sideCache != null;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initCache();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
