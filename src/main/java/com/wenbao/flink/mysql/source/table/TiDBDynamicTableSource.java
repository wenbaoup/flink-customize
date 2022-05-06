package com.wenbao.flink.mysql.source.table;

import com.wenbao.flink.mysql.source.TiDBSourceBuilder;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;

public class TiDBDynamicTableSource implements ScanTableSource {
    
    private final ResolvedCatalogTable table;
    private final ChangelogMode changelogMode;
    private int[] projectedFields;
    
    
    public TiDBDynamicTableSource(ResolvedCatalogTable table,
            ChangelogMode changelogMode) {
        this.table = table;
        this.changelogMode = changelogMode;
    }
    
    @Override
    public ChangelogMode getChangelogMode() {
        return changelogMode;
    }
    
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        /* Disable metadata as it doesn't work with projection push down at this time */
        return SourceProvider.of(
                new TiDBSourceBuilder(table, scanContext::createTypeInformation, projectedFields)
                        .build());
    }
    
    @Override
    public DynamicTableSource copy() {
        TiDBDynamicTableSource otherSource =
                new TiDBDynamicTableSource(table, changelogMode);
        otherSource.projectedFields = this.projectedFields;
        return otherSource;
    }
    
    @Override
    public String asSummaryString() {
        return "";
    }
    
    
}