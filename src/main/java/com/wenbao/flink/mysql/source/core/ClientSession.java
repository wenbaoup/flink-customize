package com.wenbao.flink.mysql.source.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.wenbao.flink.mysql.source.base.MysqlRequest;
import com.wenbao.flink.mysql.source.base.TiColumnInfo;
import com.wenbao.flink.mysql.source.base.TiTableInfo;
import com.wenbao.flink.mysql.source.iterator.CoprocessorIterator;
import com.wenbao.flink.mysql.source.row.ObjectRowImpl;
import com.wenbao.flink.mysql.source.row.Row;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.wenbao.flink.mysql.source.core.ClientConfig.MYSQL_DRIVER_NAME;
import static java.util.Objects.requireNonNull;

@Slf4j
public final class ClientSession implements AutoCloseable {
    
    private static final Set<String> BUILD_IN_DATABASES = ImmutableSet.of(
            "information_schema",
            "metrics_schema",
            "performance_schema",
            "mysql"
    );
    
    
    private static final String QUERY_META_DATA = "select * from information_schema.columns where table_schema = ? and table_name = ?";
    
    //左闭右开保证数据不重复查询
    private static final String QUERY_DATA = "select * from {0} where {1} >= ? and {1} < ?";
    
    private static final String QUERY_LAST_DATA = "select * from {0} where {1} = ?";
    
    
    private static final String QUERY_MAX_MIN = "select min({1}) as min, max({1}) as max from {0} ";
    
    private static final String QUERY_MAX_LIMIT = "SELECT MAX({1}) AS max FROM (SELECT {1} FROM {0} WHERE {1} > ? and {1} <= ? order by {1} limit ? ) tmp";
    
    private static final String MAX = "max";
    
    private static final String MIN = "min";
    
    private static final String FIELD_TYPE_VARCHAR = "varchar";
    
    
    private final ClientConfig config;
    
    private HikariDataSource dataSource;
    
    private ClientSession(ClientConfig config) {
        this.config = requireNonNull(config, "config is null");
        dataSource = getDataSource(config);
    }
    
    private HikariDataSource getDataSource(ClientConfig config) {
        return new HikariDataSource(new HikariConfig() {
            {
                setJdbcUrl(requireNonNull(config.getDatabaseUrl(), "database url can not be null"));
                setUsername(requireNonNull(config.getUsername(), "username can not be null"));
                setPassword(config.getPassword());
                setDriverClassName(MYSQL_DRIVER_NAME);
                setMaximumPoolSize(config.getMaximumPoolSize());
                setMinimumIdle(config.getMinimumIdleSize());
            }
        });
    }
    
    public List<String> getSchemaNames() {
        String sql = "SHOW DATABASES";
        try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()
        ) {
            ResultSet resultSet = statement.executeQuery(sql);
            List<String> databaseNames = new ArrayList<>();
            while (resultSet.next()) {
                String databaseName = resultSet.getString(1).toLowerCase();
                if (BUILD_IN_DATABASES.contains(databaseName)) {
                    continue;
                }
                databaseNames.add(databaseName);
            }
            return databaseNames;
        } catch (Exception e) {
            log.error("Execute sql {} fail", sql, e);
            throw new IllegalStateException(e);
        }
    }
    
    public List<String> getTableNames(String schema) {
        String sql = "SHOW TABLES";
        requireNonNull(schema, "schema is null");
        try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()
        ) {
            statement.execute("USE " + schema);
            ResultSet resultSet = statement.executeQuery(sql);
            List<String> tableNames = new ArrayList<>();
            while (resultSet.next()) {
                tableNames.add(resultSet.getString(1).toLowerCase());
            }
            return tableNames;
        } catch (Exception e) {
            log.error("Execute sql {} fail", sql, e);
            throw new IllegalStateException(e);
        }
    }
    
    //    public Optional<TiTableInfo> getTable(TableHandleInternal handle) {
//        return getTable(handle.getSchemaName(), handle.getTableName());
//    }
//
    public Optional<TiTableInfo> getTable(String schema, String tableName) {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        return Optional.ofNullable(getTiTableInfo(schema, tableName));
    }
    
    private TiTableInfo getTiTableInfo(String schema, String tableName) {
        TiTableInfo tiTableInfo = null;
        Connection jdbcConnection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            jdbcConnection = getJdbcConnection();
            ps = jdbcConnection.prepareStatement(QUERY_META_DATA);
            ps.setString(1, schema);
            ps.setString(2, tableName);
            rs = ps.executeQuery();
            tiTableInfo = new TiTableInfo();
            List<TiColumnInfo> tiColumnInfoList = new ArrayList<>();
            Map<String, TiColumnInfo> map = new HashMap<>();
            while (rs.next()) {
                TiColumnInfo tiColumnInfo = new TiColumnInfo();
                String columnName = rs.getString("COLUMN_NAME");
                tiColumnInfo.setName(columnName);
                String dataType = rs.getString("DATA_TYPE");
                tiColumnInfo.setType(dataType);
                String columnKey = rs.getString("COLUMN_KEY");
                tiColumnInfo.setPrimaryKey("PRI".equals(columnKey));
                tiColumnInfoList.add(tiColumnInfo);
                map.put(columnName, tiColumnInfo);
                //默认获取第一个主键
                if (tiColumnInfo.isPrimaryKey() && null == tiTableInfo.getPrimaryKeyColumn()) {
                    tiTableInfo.setPrimaryKeyColumn(tiColumnInfo);
                }
            }
            tiTableInfo.setColumns(tiColumnInfoList);
            tiTableInfo.setColumnsMap(map);
            tiTableInfo.setRowSize(tiColumnInfoList.size());
            tiTableInfo.setName(tableName);
        } catch (SQLException throwables) {
            log.error("mysql connection fail e:{}", throwables.getMessage());
        } finally {
            closeJdbcConnection(rs, ps, jdbcConnection);
        }
        return tiTableInfo;
        
    }
    
    private void closeJdbcConnection(ResultSet rs, PreparedStatement ps, Connection connection) {
        try {
            if (null != rs) {
                rs.close();
            }
            if (null != ps) {
                ps.close();
            }
            if (null != connection) {
                connection.close();
            }
            
        } catch (SQLException throwables) {
            log.error("mysql close fail e:{}", throwables.getMessage());
        }
    }
    
    public TiTableInfo getTableMust(TableHandleInternal handle) {
        return getTableMust(handle.getSchemaName(), handle.getTableName());
    }
    
    public TiTableInfo getTableMust(String schema, String tableName) {
        return getTable(schema, tableName).orElseThrow(
                () -> new IllegalStateException(
                        "Table " + schema + "." + tableName + " no longer exists"));
    }
    
    //    public Map<String, List<String>> listTables(Optional<String> schemaName) {
//        List<String> schemaNames = schemaName
//                .map(s -> (List<String>) ImmutableList.of(s))
//                .orElseGet(this::getSchemaNames);
//        return schemaNames.stream().collect(toImmutableMap(identity(), this::getTableNames));
//    }
//
    private List<ColumnHandleInternal> selectColumns(
            List<ColumnHandleInternal> allColumns, Stream<String> columns) {
        final Map<String, ColumnHandleInternal> columnsMap =
                allColumns.stream().collect(
                        Collectors.toMap(ColumnHandleInternal::getName, Function.identity()));
        return columns.map(columnsMap::get).collect(Collectors.toList());
    }
    
    private static List<ColumnHandleInternal> getTableColumns(TiTableInfo table) {
        return Streams.mapWithIndex(table.getColumns().stream(),
                (column, i) -> new ColumnHandleInternal(column.getName(), column.getType(),
                        (int) i))
                .collect(toImmutableList());
    }
    
    public Optional<List<ColumnHandleInternal>> getTableColumns(String schema, String tableName) {
        return getTable(schema, tableName).map(ClientSession::getTableColumns);
    }
    
    private Optional<List<ColumnHandleInternal>> getTableColumns(String schema, String tableName,
            Stream<String> columns) {
        return getTableColumns(schema, tableName).map(r -> selectColumns(r, columns));
    }
    
    //    public Optional<List<ColumnHandleInternal>> getTableColumns(String schema, String tableName,
//            List<String> columns) {
//        return getTableColumns(schema, tableName, columns.stream());
//    }
//
    public Optional<List<ColumnHandleInternal>> getTableColumns(String schema, String tableName,
            String[] columns) {
        return getTableColumns(schema, tableName, Arrays.stream(columns));
    }
    
    //
//    public Optional<List<ColumnHandleInternal>> getTableColumns(TableHandleInternal tableHandle) {
//        return getTableColumns(tableHandle.getSchemaName(), tableHandle.getTableName());
//    }
//
//    public Optional<List<ColumnHandleInternal>> getTableColumns(TableHandleInternal tableHandle,
//            List<String> columns) {
//        return getTableColumns(tableHandle.getSchemaName(), tableHandle.getTableName(), columns);
//    }
//
//    private List<RangeSplitter.RegionTask> getRangeRegionTasks(ByteString startKey,
//            ByteString endKey) {
//        List<Coprocessor.KeyRange> keyRanges =
//                ImmutableList.of(KeyRangeUtils.makeCoprocRange(startKey, endKey));
//        return RangeSplitter.newSplitter(session.getRegionManager()).splitRangeByRegion(keyRanges);
//    }
//
//    private List<RangeSplitter.RegionTask> getRangeRegionTasks(Base64KeyRange range) {
//        ByteString startKey = ByteString.copyFrom(Base64.getDecoder().decode(range.getStartKey()));
//        ByteString endKey = ByteString.copyFrom(Base64.getDecoder().decode(range.getEndKey()));
//        return getRangeRegionTasks(startKey, endKey);
//    }
//
//    private List<RangeSplitter.RegionTask> getTableRegionTasks(TableHandleInternal tableHandle) {
//        return getTable(tableHandle)
//                .map(table -> table.isPartitionEnabled()
//                        ? table.getPartitionInfo().getDefs().stream().map(TiPartitionDef::getId)
//                        .collect(Collectors.toList()) : ImmutableList.of(table.getId()))
//                .orElseGet(ImmutableList::of)
//                .stream()
//                .map(tableId -> getRangeRegionTasks(RowKey.createMin(tableId).toByteString(),
//                        RowKey.createBeyondMax(tableId).toByteString()))
//                .flatMap(Collection::stream)
//                .collect(Collectors.toList());
//    }
    
    public List<KeyRange> getTableRanges(TableHandleInternal tableHandle,
            TiColumnInfo primaryKeyColumn, Long batchSize) {
//        System.out.println("11111111111L" + Thread.currentThread().getName());
        String queryMaxMin = MessageFormat.format(QUERY_MAX_MIN, tableHandle.getTableName(),
                primaryKeyColumn.getName());
        KeyRange keyRange = null;
        //都是[startKey,endKey)
        List<KeyRange> rangeList = new ArrayList<>();
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            connection = getJdbcConnection();
            ps = connection.prepareStatement(queryMaxMin);
            rs = ps.executeQuery();
            while (rs.next()) {
                if (null == rs.getString(MIN)) {
                    //表最开始的时候是空表
                    keyRange = new KeyRange("0", "1");
                } else {
                    keyRange = new KeyRange(rs.getString(MIN), rs.getString(MAX));
                    
                }
            }
            if (null == keyRange) {
                log.error("getTableRanges error KeyRange is null");
                return rangeList;
            }
            if (primaryKeyColumn.getType().equalsIgnoreCase(FIELD_TYPE_VARCHAR)) {
                String queryLimit = MessageFormat.format(QUERY_MAX_LIMIT,
                        tableHandle.getTableName(),
                        primaryKeyColumn.getName());
                ps = connection.prepareStatement(queryLimit);
                //最小不等于最大说明还有数据
                while (!keyRange.getStartKey().equals(keyRange.getEndKey())) {
                    ps.setString(1, keyRange.getStartKey());
                    //保证每次limit取出的数据一定比最大值小  否则可能会出现表中新增数据 此数据比最大值大
                    ps.setString(2, keyRange.getEndKey());
                    ps.setLong(3, batchSize);
                    rs = ps.executeQuery();
                    while (rs.next()) {
                        String max = rs.getString(MAX);
                        //如果本次limit取值的数据等于上次的最小值 说明此主键对应的数据超过了batchSize的大小
                        if (keyRange.getStartKey().equals(max)) {
                            throw new RuntimeException("the data corresponding to this " + max +
                                    " is greater than batchSize, order field is " +
                                    primaryKeyColumn.getName());
                        }
                        rangeList.add(new KeyRange(keyRange.getStartKey(), max));
                        //将最大值给最新值 循环重新取值
                        keyRange.setStartKey(max);
                    }
                }
                //保持string类型的[startKey,endKey)
                //当扫表到endKey的时候表中主键没有比endKey更大的值了 此时会变成[endKey,？？？)
                //所以对最后的endKey单独查询[endKey,endKey]
                rangeList.add(new KeyRange(keyRange.getEndKey(), keyRange.getEndKey()));
            } else {
                getLongKeyRange(keyRange, rangeList, batchSize);
            }
            
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            closeJdbcConnection(rs, ps, connection);
        }
        return rangeList;
    }
    
    
    private void getLongKeyRange(KeyRange keyRange, List<KeyRange> rangeList, Long batchSize) {
        long start = Long.parseLong(keyRange.getStartKey());
        long end = Long.parseLong(keyRange.getEndKey());
        for (long i = start; i <= end; i = i + batchSize) {
            rangeList.add(new KeyRange(String.valueOf(i), String.valueOf(i + batchSize)));
        }
        KeyRange lastKeyRange = rangeList.get(rangeList.size() - 1);
        if (Long.parseLong(lastKeyRange.getEndKey()) > Long.parseLong(keyRange.getEndKey())) {
            //左闭右开
            lastKeyRange.setEndKey(String.valueOf(end + 1));
        }
    }
    
    public MysqlRequest.MysqlRequestBuilder request(TableHandleInternal table, List<String> columns) {
        TiTableInfo tableInfo = getTableMust(table);
        if (columns.isEmpty()) {
            columns = ImmutableList.of(tableInfo.getColumns().get(0).getName());
        }
        return MysqlRequest.builder()
                .tableInfo(tableInfo)
                .requiredCols(columns);
    }
    
    public CoprocessorIterator<Row> iterate(MysqlRequest request, KeyRange range,
            List<ColumnHandleInternal> columns) {
//        System.out.println("22222222L" + Thread.currentThread().getName());
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        TiTableInfo tableInfo = request.getTableInfo();
        List<Row> result = new ArrayList<>();
        log.info("query data table :{} primaryKeyColumn:{} KeyRange:{}", tableInfo.getName(),
                tableInfo.getPrimaryKeyColumn().getName(), range);
        try {
            connection = getJdbcConnection();
            //说明是string类型的[endKey,endKey]
            String sql;
            if (range.getStartKey().equals(range.getEndKey())) {
                sql = MessageFormat.format(QUERY_LAST_DATA, tableInfo.getName(),
                        tableInfo.getPrimaryKeyColumn().getName());
                ps = connection.prepareStatement(sql);
                ps.setString(1, range.getStartKey());
            } else {
                sql = MessageFormat.format(QUERY_DATA, tableInfo.getName(),
                        tableInfo.getPrimaryKeyColumn().getName());
                ps = connection.prepareStatement(sql);
                ps.setString(1, range.getStartKey());
                ps.setString(2, range.getEndKey());
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                Row row = ObjectRowImpl.create(columns.size());
                for (int i = 0; i < columns.size(); i++) {
                    //在获取表字段时已经做了kafka元数据的校验
                    // 不存在表中的字段columns.get(i)为null
                    if (null == columns.get(i)) {
                        row.setNull(i);
                    }
//                    String columnName =columns.get(i).getName() ;
//                    if (columnName.startsWith(MATE_TIDB_PREFIX)) {
//                        row.setNull(i);
//                    }
                    else {
                        Object object = rs.getObject(columns.get(i).getName());
                        if (null == object) {
                            row.setNull(i);
                        } else {
                            row.set(i, columns.get(i).getType(), object);
                        }
                    }
                }
                result.add(row);
            }
            return new CoprocessorIterator<>(result.iterator());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            closeJdbcConnection(rs, ps, connection);
        }
        return null;
    }
    
    public void sqlUpdate(String... sqls) {
        try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()
        ) {
            for (String sql : sqls) {
                log.info("Sql update: " + sql);
                statement.executeUpdate(sql);
            }
        } catch (Exception e) {
            log.error("Execute sql fail", e);
            throw new IllegalStateException(e);
        }
    }

//    public void createTable(String databaseName, String tableName, List<String> columnNames,
//            List<String> columnTypes, List<String> primaryKeyColumns, List<String> uniqueKeyColumns,
//            boolean ignoreIfExists) {
//        sqlUpdate(getCreateTableSql(requireNonNull(databaseName), requireNonNull(tableName),
//                requireNonNull(columnNames), requireNonNull(columnTypes), primaryKeyColumns,
//                uniqueKeyColumns, ignoreIfExists));
//    }
    
    public void dropTable(String databaseName, String tableName, boolean ignoreIfNotExists) {
        sqlUpdate(String.format("DROP TABLE %s `%s`.`%s`", ignoreIfNotExists ? "IF EXISTS" : "",
                requireNonNull(databaseName), requireNonNull(tableName)));
    }
    
    public void createDatabase(String databaseName, boolean ignoreIfExists) {
        sqlUpdate(String.format("CREATE DATABASE %s `%s`", ignoreIfExists ? "IF NOT EXISTS" : "",
                requireNonNull(databaseName)));
    }
    
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists) {
        sqlUpdate(String.format("DROP DATABASE %s `%s`", ignoreIfNotExists ? "IF EXISTS" : "",
                requireNonNull(databaseName)));
    }
    
    public boolean databaseExists(String databaseName) {
        return getSchemaNames().contains(requireNonNull(databaseName));
    }
    
    public boolean tableExists(String databaseName, String tableName) {
        return databaseExists(requireNonNull(databaseName))
                && getTableNames(databaseName).contains(requireNonNull(tableName));
    }
    
    public void renameTable(String oldDatabaseName, String newDatabaseName, String oldTableName,
            String newTableName) {
        sqlUpdate(String.format("RENAME TABLE `%s`.`%s` TO `%s`.`%s` ",
                requireNonNull(oldDatabaseName),
                requireNonNull(oldTableName),
                requireNonNull(newDatabaseName),
                requireNonNull(newTableName)));
    }
    
    public void addColumn(String databaseName, String tableName, String columnName,
            String columnType) {
        sqlUpdate(String.format("ALTER TABLE `%s`.`%s` ADD COLUMN `%s` %s",
                requireNonNull(databaseName),
                requireNonNull(tableName),
                requireNonNull(columnName),
                requireNonNull(columnType)));
    }
    
    public void renameColumn(String databaseName, String tableName, String oldName, String newName,
            String newType) {
        sqlUpdate(String.format("ALTER TABLE `%s`.`%s` CHANGE `%s` `%s` %s",
                requireNonNull(databaseName),
                requireNonNull(tableName),
                requireNonNull(oldName),
                requireNonNull(newName),
                requireNonNull(newType)));
    }
    
    public void dropColumn(String databaseName, String tableName, String columnName) {
        sqlUpdate(String.format("ALTER TABLE `%s`.`%s` DROP COLUMN `%s`",
                requireNonNull(databaseName),
                requireNonNull(tableName),
                requireNonNull(columnName)));
    }
    
    public Connection getJdbcConnection() throws SQLException {
        if (dataSource.isClosed()) {
            dataSource = getDataSource(config);
            log.info("retry get dataSource");
        }
        return dataSource.getConnection();
    }
    
    @Override
    public String toString() {
        return toStringHelper(this)
                .add("config", config)
                .toString();
    }

//    public List<String> getPrimaryKeyColumns(String databaseName, String tableName) {
//        return getTableMust(databaseName, tableName).getColumns()
//                .stream()
//                .filter(TiColumnInfo::isPrimaryKey)
//                .map(TiColumnInfo::getName)
//                .collect(Collectors.toList());
//    }
//
//    public List<String> getUniqueKeyColumns(String databaseName, String tableName) {
//        List<String> primaryKeyColumns = getPrimaryKeyColumns(databaseName, tableName);
//        return getTableMust(databaseName, tableName).getIndices().stream()
//                .filter(TiIndexInfo::isUnique)
//                .map(TiIndexInfo::getIndexColumns)
//                .flatMap(Collection::stream).map(TiIndexColumn::getName)
//                .filter(name -> !primaryKeyColumns.contains(name)).collect(Collectors.toList());
//    }
    
    
    @Override
    public synchronized void close() throws Exception {
        dataSource.close();
    }
    
    
    public static ClientSession createWithSingleConnection(ClientConfig config) {
        ClientConfig clientConfig = new ClientConfig(config);
        clientConfig.setMaximumPoolSize(1);
        clientConfig.setMinimumIdleSize(1);
        return new ClientSession(clientConfig);
    }
    
    public static ClientSession create(ClientConfig config) throws SQLException {
        return new ClientSession(new ClientConfig(config));
    }
}
