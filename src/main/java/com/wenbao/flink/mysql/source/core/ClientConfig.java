package com.wenbao.flink.mysql.source.core;

import java.util.Map;
import java.util.Objects;

public final class ClientConfig {
    
    public static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";
    
    public static final String DATABASE_URL = "tidb.database.url";
    
    public static final String USERNAME = "tidb.username";
    
    public static final String PASSWORD = "tidb.password";
    
    
    public static final String MAX_POOL_SIZE = "tidb.maximum.pool.size";
    public static final String MAX_POOL_SIZE_DEFAULT = "1";
    
    public static final String MIN_IDLE_SIZE = "tidb.minimum.idle.size";
    public static final String MIN_IDLE_SIZE_DEFAULT = "1";
    
    
    private String databaseUrl;
    
    private String username;
    
    private String password;
    
    private int maximumPoolSize;
    
    private int minimumIdleSize;
    
    
    public ClientConfig() {
        this(null,
                null,
                null,
                Integer.parseInt(MAX_POOL_SIZE_DEFAULT),
                Integer.parseInt(MIN_IDLE_SIZE_DEFAULT));
    }
    
    public ClientConfig(String databaseUrl, String username, String password) {
        this(databaseUrl,
                username,
                password,
                Integer.parseInt(MAX_POOL_SIZE_DEFAULT),
                Integer.parseInt(MIN_IDLE_SIZE_DEFAULT)
        );
    }
    
    /* For historical compatibility, this constructor omits cluster TLS
     * options and implicitly disables TLS. */
    public ClientConfig(String databaseUrl,
            String username,
            String password,
            int maximumPoolSize,
            int minimumIdleSize,
            String writeMode,
            boolean isFilterPushDown,
            String dnsSearch,
            long timeout,
            long scanTimeout,
            boolean buildInDatabaseVisible) {
        this.databaseUrl = databaseUrl;
        this.username = username;
        this.password = password;
        this.maximumPoolSize = maximumPoolSize;
        this.minimumIdleSize = minimumIdleSize;
    }
    
    /* This constructor adds support for cluster TLS options without
     * breaking backward compatibility for existing programs. */
    public ClientConfig(String databaseUrl,
            String username,
            String password,
            int maximumPoolSize,
            int minimumIdleSize) {
        this.databaseUrl = databaseUrl;
        this.username = username;
        this.password = password;
        this.maximumPoolSize = maximumPoolSize;
        this.minimumIdleSize = minimumIdleSize;
    }
    
    public ClientConfig(Map<String, String> properties) {
        this(properties.get(DATABASE_URL),
                properties.get(USERNAME),
                properties.get(PASSWORD),
                Integer.parseInt(properties.getOrDefault(MAX_POOL_SIZE, MAX_POOL_SIZE_DEFAULT)),
                Integer.parseInt(properties.getOrDefault(MIN_IDLE_SIZE, MIN_IDLE_SIZE_DEFAULT))
        );
    }
    
    public ClientConfig(ClientConfig config) {
        this(config.getDatabaseUrl(),
                config.getUsername(),
                config.getPassword(),
                config.getMaximumPoolSize(),
                config.getMinimumIdleSize()
        );
    }
    
    public String getDatabaseUrl() {
        return databaseUrl;
    }
    
    public void setDatabaseUrl(String databaseUrl) {
        this.databaseUrl = databaseUrl;
    }
    
    public String getUsername() {
        return username;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    
    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }
    
    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
    }
    
    public int getMinimumIdleSize() {
        return minimumIdleSize;
    }
    
    public void setMinimumIdleSize(int minimumIdleSize) {
        this.minimumIdleSize = minimumIdleSize;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClientConfig that = (ClientConfig) o;
        return maximumPoolSize == that.maximumPoolSize
                && minimumIdleSize == that.minimumIdleSize
                && Objects.equals(databaseUrl, that.databaseUrl)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(databaseUrl, username, password,
                maximumPoolSize,
                minimumIdleSize);
    }
    
    @Override
    public String toString() {
        return "ClientConfig{"
                + ", databaseUrl='" + databaseUrl + '\''
                + ", username='" + username + '\''
                + ", password='" + password + '\''
                + ", maximumPoolSize=" + maximumPoolSize
                + ", minimumIdleSize=" + minimumIdleSize
                + '}';
    }
}
