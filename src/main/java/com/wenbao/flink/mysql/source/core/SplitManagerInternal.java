package com.wenbao.flink.mysql.source.core;

import com.wenbao.flink.mysql.source.base.TiColumnInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

@Slf4j
public final class SplitManagerInternal {
    
    
    private final ClientSession session;
    
    public SplitManagerInternal(ClientSession session) {
        this.session = requireNonNull(session, "session is null");
    }
    
    public List<SplitInternal> getSplits(TableHandleInternal tableHandle,
            TiColumnInfo primaryKeyColumn, Long batchSize) {
        List<SplitInternal> splits = session.getTableRanges(tableHandle, primaryKeyColumn,
                batchSize)
                .stream()
                .map(range -> new SplitInternal(tableHandle, range))
                .collect(toCollection(ArrayList::new));
        Collections.shuffle(splits);
        log.info("The number of split for table `{}`.`{}` is {}", tableHandle.getSchemaName(),
                tableHandle.getTableName(), splits.size());
        return Collections.unmodifiableList(splits);
    }
    
    @Override
    public String toString() {
        return toStringHelper(this)
                .add("session", session)
                .toString();
    }
}