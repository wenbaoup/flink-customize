package com.wenbao.flink.mysql.source.table;

import com.wenbao.flink.mysql.source.TiDBOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.util.Set;

import static com.wenbao.flink.mysql.source.TiDBOptions.STREAMING_SOURCE;

public class TiDBDynamicTableFactory implements DynamicTableSourceFactory {
    
    public static final String IDENTIFIER = "tidb";
    
    /**
     * see ${@link org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory}
     */
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
            .key("sink.buffer-flush.max-rows")
            .intType()
            .defaultValue(100)
            .withDescription(
                    "the flush max size (includes all append, upsert and delete records), over this number"
                            + " of records, will flush data. The default value is 100.");
    private static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
            .key("sink.buffer-flush.interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(1))
            .withDescription(
                    "the flush interval mills, over this time, asynchronous threads will flush data. The "
                            + "default value is 1s.");
    public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
            .key("sink.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("the max retry times if writing records to database failed.");
    // look up config options
    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
            .key("lookup.cache.max-rows")
            .longType()
            .defaultValue(-1L)
            .withDescription("the max number of rows of lookup cache, over this value, "
                    +
                    "the oldest rows will be eliminated. \"cache.max-rows\" and \"cache.ttl\" options "
                    + "must all be specified if any of them is specified. "
                    + "Cache is not enabled as default.");
    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL = ConfigOptions
            .key("lookup.cache.ttl")
            .durationType()
            .defaultValue(Duration.ofSeconds(10))
            .withDescription("the cache time to live.");
    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions
            .key("lookup.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("the max retry times if lookup database failed.");
    
    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
    
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return TiDBOptions.requiredOptions();
    }
    
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        return new TiDBDynamicTableSource(context.getCatalogTable(),
                config.getOptional(STREAMING_SOURCE).isPresent() ? ChangelogMode.all()
                        : ChangelogMode.insertOnly());
    }
    
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return TiDBOptions.withMoreOptionalOptions(
                SINK_BUFFER_FLUSH_INTERVAL,
                SINK_BUFFER_FLUSH_MAX_ROWS,
                SINK_MAX_RETRIES);
    }
}