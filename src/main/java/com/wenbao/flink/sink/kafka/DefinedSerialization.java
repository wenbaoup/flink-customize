package com.wenbao.flink.sink.kafka;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 自定义key，已经自定义key和value的序列化方式
 *
 * @Author : WenBao
 * Date : 10:11 2018/6/25
 */
public class DefinedSerialization extends AbstractSerialization<Tuple2<String, String>> {


    private static final long serialVersionUID = -6095174255432490089L;


    @Override
    public byte[] serializeKey(Tuple2<String, String> element) {
        return element.f0.getBytes();
    }

    @Override
    public byte[] serializeValue(Tuple2<String, String> element) {
        return element.f1.getBytes();
    }
}
