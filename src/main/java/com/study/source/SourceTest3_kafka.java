package com.study.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class SourceTest3_kafka {
    public static void main(String[] args) throws Exception {

        // 1 配置文件
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "debugboxreset1008.sa:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 开启自动提交
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000); // 提交offset时间间隔
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test"); // 消费者组
        // 注意只有新组或者offset丢失时，才会初始化offset的值，它默认是最新的。因此新加的组默认都是从最新的消费位置开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 这里手动设置从最旧的数据开始消费

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> kafkaStreamSource = env.addSource(new FlinkKafkaConsumer011<String>("event_topic", new SimpleStringSchema(), properties));
        kafkaStreamSource.print();
        env.execute();
    }
}
