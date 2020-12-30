package com.study.sink;

import com.study.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class Test1_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/sensor.txt");
        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] strings = s.split(",");
                return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim())).toString();
            }
        });
        map.addSink(new FlinkKafkaProducer011<String>("localhost:9092", "sinkTest", new SimpleStringSchema()));
        env.execute();

    }
}
