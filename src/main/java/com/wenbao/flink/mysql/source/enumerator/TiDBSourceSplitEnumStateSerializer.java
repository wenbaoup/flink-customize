package com.wenbao.flink.mysql.source.enumerator;

import com.wenbao.flink.mysql.source.split.TiDBSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class TiDBSourceSplitEnumStateSerializer
        implements SimpleVersionedSerializer<TiDBSourceSplitEnumState> {
    
    public static final int CURRENT_VERSION = 0;
    
    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }
    
    @Override
    public byte[] serialize(TiDBSourceSplitEnumState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
            for (TiDBSourceSplit split : state.assignedSplits()) {
                split.serialize(dos);
            }
            dos.flush();
            return baos.toByteArray();
        }
    }
    
    @Override
    public TiDBSourceSplitEnumState deserialize(int version, byte[] bytes) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException(
                    String.format(
                            "The bytes are serialized with version %d, "
                                    + "while this deserializer only supports version up to %d",
                            version, CURRENT_VERSION));
        }
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            Set<TiDBSourceSplit> splits = new HashSet<>();
            while (dis.available() > 0) {
                splits.add(TiDBSourceSplit.deserialize(dis));
            }
            return new TiDBSourceSplitEnumState(splits);
        }
    }
}
