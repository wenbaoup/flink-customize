package com.wenbao.flink.mysql.source.core;

import com.wenbao.flink.mysql.source.base.MysqlRequest;
import com.wenbao.flink.mysql.source.iterator.CoprocessorIterator;
import com.wenbao.flink.mysql.source.row.Row;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class RecordSetInternal {
    
    private final List<ColumnHandleInternal> columnHandles;
    private final List<String> columnTypes;
    private final CoprocessorIterator<Row> iterator;
    
    public RecordSetInternal(ClientSession session, SplitInternal split,
            List<ColumnHandleInternal> columnHandles, Optional<String> expression) {
        requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.columnTypes = columnHandles.stream().filter(Objects::nonNull).map(ColumnHandleInternal::getType)
                .collect(toImmutableList());
        List<String> columns = columnHandles.stream().filter(Objects::nonNull).map(ColumnHandleInternal::getName)
                .collect(toImmutableList());
        MysqlRequest.MysqlRequestBuilder request = session.request(split.getTable(), columns);
        request.expression(expression.orElse(null));
        iterator = session.iterate(request.build(),
                new KeyRange(split.getStartKey(), split.getEndKey()), columnHandles);
    }
    
    public List<String> getColumnTypes() {
        return columnTypes;
    }
    
    public RecordCursorInternal cursor() {
        return new RecordCursorInternal(columnHandles, iterator);
    }
}
