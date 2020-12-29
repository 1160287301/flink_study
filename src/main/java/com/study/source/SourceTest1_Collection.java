package com.study.source;

import com.study.beans.SensorReading;
import lombok.val;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 打印集合
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.fromCollection(Arrays.asList(
                new SensorReading("s_1", 123123123L, 34.5),
                new SensorReading("s_2", 123123124L, 35.5),
                new SensorReading("s_2", 123123125L, 36.5),
                new SensorReading("s_2", 123123126L, 37.5)
        ));
        sensorReadingDataStreamSource.print("sensors");
        // 打印元素
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);
        integerDataStreamSource.print("element");
        env.execute("my flink streaming job name");
    }
}
