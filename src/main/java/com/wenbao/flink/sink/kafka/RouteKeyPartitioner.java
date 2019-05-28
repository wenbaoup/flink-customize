package com.wenbao.flink.sink.kafka;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义分区的key，分区算法
 *
 * @Author : WenBao
 * Date : 10:11 2018/6/25
 */
public class RouteKeyPartitioner extends FlinkKafkaPartitioner<Tuple2<String, String>> {


    private static final long serialVersionUID = 3840561211176611143L;
    private AtomicInteger atomicInteger = new AtomicInteger(0);

    @Override
    public int partition(Tuple2<String, String> record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        if (atomicInteger.get() >= 100) {
            atomicInteger.set(0);
        }
        return atomicInteger.incrementAndGet() % partitions.length;
    }
}
