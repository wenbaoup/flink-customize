package com.wenbao.flink.mysql.source.enumerator;

import com.wenbao.flink.mysql.source.base.TiColumnInfo;
import com.wenbao.flink.mysql.source.base.TiTableInfo;
import com.wenbao.flink.mysql.source.core.*;
import com.wenbao.flink.mysql.source.split.TiDBSourceSplit;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

public class TiDBSourceSplitEnumerator implements
        SplitEnumerator<TiDBSourceSplit, TiDBSourceSplitEnumState> {
    
    private static final Logger LOG = LoggerFactory.getLogger(TiDBSourceSplitEnumerator.class);
    
    private final Map<String, String> properties;
    private final SplitEnumeratorContext<TiDBSourceSplit> context;
    
    private final Map<Integer, Set<TiDBSourceSplit>> pendingSplitAssignment;
    private final Set<Integer> assignedReaders;
    private final Set<Integer> notifiedReaders;
    private final Set<TiDBSourceSplit> assignedSplits;
    
    public TiDBSourceSplitEnumerator(
            Map<String, String> properties,
            SplitEnumeratorContext<TiDBSourceSplit> context) {
        this(properties, context, Collections.emptySet());
    }
    
    public TiDBSourceSplitEnumerator(
            Map<String, String> properties,
            SplitEnumeratorContext<TiDBSourceSplit> context,
            Set<TiDBSourceSplit> assignedSplits) {
        this.properties = properties;
        this.context = context;
        this.assignedSplits = new HashSet<>(assignedSplits);
        this.pendingSplitAssignment = new HashMap<>();
        this.assignedReaders = new HashSet<>();
        this.notifiedReaders = new HashSet<>();
        initPendingSplitAssignment();
    }
    
    private void assignPendingSplits(Set<Integer> pendingReaders) {
        Map<Integer, List<TiDBSourceSplit>> incrementalAssignment = new HashMap<>();
        
        // Check if there's any pending splits for given readers
        for (int pendingReader : pendingReaders) {
            // Remove pending assignment for the reader
            final Set<TiDBSourceSplit> pendingAssignmentForReader =
                    pendingSplitAssignment.remove(pendingReader);
            
            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Put pending assignment into incremental assignment
                incrementalAssignment
                        .computeIfAbsent(pendingReader, (key) -> new ArrayList<>())
                        .addAll(pendingAssignmentForReader);
                
                // Make pending partitions as already assigned
                assignedSplits.addAll(pendingAssignmentForReader);
            }
            assignedReaders.add(pendingReader);
        }
        
        // Assign pending splits to readers
        if (!incrementalAssignment.isEmpty()) {
            LOG.info("Assigning splits to readers {}", incrementalAssignment);
            context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
        }
        
        for (int reader : assignedReaders) {
            if (notifiedReaders.contains(reader) ||
                    !context.registeredReaders().containsKey(reader)) {
                continue;
            }
            context.signalNoMoreSplits(reader);
            notifiedReaders.add(reader);
        }
    }
    
    public void initPendingSplitAssignment() {
        try (ClientSession splitSession = ClientSession
                .createWithSingleConnection(new ClientConfig(properties))) {
            // check exist
            final String databaseName = properties.get(TIDBConfigConstants.TIDB_DATABASE_NAME);
            final String tableName = properties.get(TIDBConfigConstants.TIDB_TABLE_NAME);
            final Long batchSize = Long.valueOf(
                    properties.get(TIDBConfigConstants.TIDB_BATCH_SIZE));
            String orderColumn = properties.get(TIDBConfigConstants.TIDB_TABLE_ORDER_COLUMN);
            TiTableInfo tableInfo = splitSession.getTableMust(databaseName, tableName);
            if (null != orderColumn) {
                changePrimaryKeyColumn(orderColumn, tableInfo);
            }
            final TableHandleInternal tableHandleInternal = new TableHandleInternal(
                    UUID.randomUUID().toString(), databaseName, tableName);
            List<SplitInternal> splits =
                    new SplitManagerInternal(splitSession).getSplits(tableHandleInternal,
                            tableInfo.getPrimaryKeyColumn(), batchSize);
            List<TiDBSourceSplit> allSplits = splits.stream().map(TiDBSourceSplit::new)
                    .collect(Collectors.toList());
            int parallelism = context.currentParallelism();
            for (int i = 0; i < allSplits.size(); i++) {
                int reader = i % parallelism;
                pendingSplitAssignment.computeIfAbsent(reader, integer -> new HashSet<>())
                        .add(allSplits.get(i));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    
    private void changePrimaryKeyColumn(String orderColumn, TiTableInfo tableInfo) {
        TiColumnInfo tiColumnInfo = tableInfo.getColumnsMap().get(orderColumn);
        if (null != tiColumnInfo) {
            //人为指定的排序列高于查询元数据中的主键字段  主要是兼容查询doris时没有主键信息
            tableInfo.setPrimaryKeyColumn(tiColumnInfo);
        }
    }
    
    @Override
    public void start() {
    }
    
    
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostName) {
    }
    
    @Override
    public void addSplitsBack(List<TiDBSourceSplit> splits, int subtaskId) {
        this.pendingSplitAssignment.computeIfAbsent(subtaskId, key -> new HashSet<>())
                .addAll(splits);
        this.notifiedReaders.remove(subtaskId);
    }
    
    @Override
    public void addReader(int subtaskId) {
        LOG.debug("Adding reader {} to TiDBSourceSplitEnumerator", subtaskId);
        assignPendingSplits(Collections.singleton(subtaskId));
    }
    
    @Override
    public TiDBSourceSplitEnumState snapshotState(long l) throws Exception {
        return new TiDBSourceSplitEnumState(assignedSplits);
    }
    
    @Override
    public void close() {
    }
    
    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent event) {
        SplitEnumerator.super.handleSourceEvent(subtaskId, event);
    }
}
