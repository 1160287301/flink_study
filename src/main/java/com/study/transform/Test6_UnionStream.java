package com.study.transform;

import com.study.beans.SensorReading;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * 合并多条数据类型一样的流
 */
public class Test6_UnionStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/sensor.txt");
        SingleOutputStreamOperator<SensorReading> map = streamSource.map(s -> {
            String[] strings = s.split(",");
            return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
        });
        // 根据温度大小给每条数据打上标签, 然后再用select方法选择
        SplitStream<SensorReading> split = map.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return (sensorReading.getTemperature() > 4) ? Collections.singletonList("high") : Collections.singletonList("lower");
            }
        });
        DataStream<SensorReading> high = split.select("high");
        DataStream<SensorReading> lower = split.select("lower");
        DataStream<SensorReading> all = split.select("high", "lower");
        high.union(lower, all).print();
        env.execute();
    }
}
