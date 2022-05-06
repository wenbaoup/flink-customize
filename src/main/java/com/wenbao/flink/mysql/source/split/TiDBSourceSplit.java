package com.wenbao.flink.mysql.source.split;

import com.wenbao.flink.mysql.source.core.SplitInternal;
import com.wenbao.flink.mysql.source.core.TableHandleInternal;
import org.apache.flink.api.connector.source.SourceSplit;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class TiDBSourceSplit implements SourceSplit {
    
    private final SplitInternal split;
    
    public TiDBSourceSplit(SplitInternal split) {
        this.split = split;
    }
    
    @Override
    public String splitId() {
        return split.toString();
    }
    
    public SplitInternal getSplit() {
        return split;
    }
    
    @Override
    public int hashCode() {
        return split.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TiDBSourceSplit)) {
            return false;
        }
        return Objects.equals(split, ((TiDBSourceSplit) o).split);
    }
    
    public void serialize(DataOutputStream dos) throws IOException {
        TableHandleInternal table = split.getTable();
        dos.writeUTF(table.getConnectorId());
        dos.writeUTF(table.getSchemaName());
        dos.writeUTF(table.getTableName());
        dos.writeUTF(split.getStartKey());
        dos.writeUTF(split.getEndKey());
    }
    
    public static TiDBSourceSplit deserialize(DataInputStream dis) throws IOException {
        String connectorId = dis.readUTF();
        String schemaName = dis.readUTF();
        String tableName = dis.readUTF();
        String startKey = dis.readUTF();
        String endKey = dis.readUTF();
        return new TiDBSourceSplit(
                new SplitInternal(new TableHandleInternal(connectorId, schemaName, tableName),
                        startKey, endKey));
    }
    
    @Override
    public String toString() {
        return toStringHelper(this)
                .add("splitInternal", split)
                .toString();
    }
}
