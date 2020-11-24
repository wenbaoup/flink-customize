package com.wenbao.flink.kudu;

import com.alibaba.fastjson.JSON;
import com.wenbao.flink.kudu.connector.KuduTableInfo;
import com.wenbao.flink.kudu.pojo.TestPojo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.junit.Test;

import java.util.Properties;


public class KuduSinkTest {

    @Test
    public void test1() throws Exception {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink_1");
        StreamExecutionEnvironment ENV =
                StreamExecutionEnvironment.getExecutionEnvironment();
        ENV.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> kafkaSource = ENV.addSource(
                new FlinkKafkaConsumer011<>(
                        "test",
                        new SimpleStringSchema(),
                        props
                )
        );
        KuduTableInfo kuduTableInfo = KuduTableInfo.Builder.open("sink_test").build();
        kafkaSource.map((MapFunction<String, TestPojo>) value -> JSON.parseObject(value, TestPojo.class))
                .addSink(new KuduSink<TestPojo>("localhost", kuduTableInfo, TestPojo.class).withStrongConsistency());
        ENV.execute();
    }

}
