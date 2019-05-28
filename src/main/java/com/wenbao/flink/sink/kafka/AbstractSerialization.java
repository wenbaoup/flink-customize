package com.wenbao.flink.sink.kafka;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public abstract class AbstractSerialization<T> implements KeyedSerializationSchema<Tuple2<String, String>> {

    private static final long serialVersionUID = -6143371974102617590L;


    @Override
    public String getTargetTopic(Tuple2<String, String> element) {
        return null;
    }
}
