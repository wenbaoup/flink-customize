package com.wenbao.flink.mysql.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class TiDBSourceSplitSerializer implements SimpleVersionedSerializer<TiDBSourceSplit> {
    
    @Override
    public int getVersion() {
        return 0;
    }
    
    @Override
    public byte[] serialize(TiDBSourceSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
            split.serialize(dos);
            dos.flush();
            return baos.toByteArray();
        }
    }
    
    @Override
    public TiDBSourceSplit deserialize(int version, byte[] bytes) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInputStream dis = new DataInputStream(bais)) {
            return TiDBSourceSplit.deserialize(dis);
        }
    }
}
