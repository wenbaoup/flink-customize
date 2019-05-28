package com.wenbao.flink.sink.kafka;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.Serializable;
import java.util.Optional;
import java.util.Properties;

/**
 * 将消息发送到Kafka
 *
 * @Author : WenBao
 * Date : 10:10 2018/6/25
 */
public class SendToKafka implements Serializable {

    private static final long serialVersionUID = -1671283556024049412L;

    private static Properties properties;

    static {
        properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
    }

    /**
     * 将flinkKafkaPartitioner 分区算法精确到每一个任务
     *
     * @param dataStream            需要发送的流消息
     * @param topic                 发送的Topic
     * @param flinkKafkaPartitioner 分区算法精确到每一个任务
     */
    public void kafkaSend(DataStream<Tuple2<String, String>> dataStream, String topic, FlinkKafkaPartitioner<Tuple2<String, String>> flinkKafkaPartitioner) {
        dataStream.addSink(new FlinkKafkaProducer011<>(topic,
                new DefinedSerialization(),
                properties,
                Optional.of(flinkKafkaPartitioner))).name("sinkToKafka");
    }

}
