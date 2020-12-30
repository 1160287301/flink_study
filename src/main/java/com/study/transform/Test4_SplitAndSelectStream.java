package com.study.transform;

import com.study.beans.SensorReading;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * 使用split和select进行分流操作
 */
public class Test4_SplitAndSelectStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/sensor.txt");
        SingleOutputStreamOperator<SensorReading> map = streamSource.map(s -> {
            String[] strings = s.split(",");
            return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
        });
        // 根据温度大小给每条数据打上标签, 然后再用select方法选择
        map.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return (sensorReading.getTemperature() > 4) ? Collections.singletonList("high") : Collections.singletonList("lower");
            }
        })
//                .select("high")
                .select("high", "lower")
                .print();
        env.execute();
    }
}
