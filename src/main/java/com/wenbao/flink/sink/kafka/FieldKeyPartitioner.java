package com.wenbao.flink.sink.kafka;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * 自定义分区的key，分区算法
 *
 * @Author : WenBao
 * Date : 10:11 2018/6/25
 */
public class FieldKeyPartitioner extends FlinkKafkaPartitioner<Tuple2<String, String>> {


    private static final long serialVersionUID = 3840561211176611143L;

    @Override
    public int partition(Tuple2<String, String> record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        return record.f0.hashCode() % partitions.length;
    }
}
