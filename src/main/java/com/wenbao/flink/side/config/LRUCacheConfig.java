package com.wenbao.flink.side.config;

import java.io.Serializable;

public class LRUCacheConfig implements Serializable {

    private static final long serialVersionUID = 486530293045463463L;
    private Long cacheSize;

    private Long cacheTimeout;


    public LRUCacheConfig(Long cacheSize, Long cacheTimeout) {
        this.cacheSize = cacheSize;
        this.cacheTimeout = cacheTimeout;
    }

    public Long getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(Long cacheSize) {
        this.cacheSize = cacheSize;
    }

    public Long getCacheTimeout() {
        return cacheTimeout;
    }

    public void setCacheTimeout(Long cacheTimeout) {
        this.cacheTimeout = cacheTimeout;
    }

    @Override
    public String toString() {
        return "LRUCacheConfig{" +
                "cacheSize=" + cacheSize +
                ", cacheTTLMs=" + cacheTimeout +
                '}';
    }
}
