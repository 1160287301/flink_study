package com.study.temporalSemantics;

import com.study.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class Test1_EventTime {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(300);  // 周期生成watermark的时间周期,单位毫秒, 默认是200毫秒
        // 设置时间语义为事件发生时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<SensorReading> map = env.readTextFile("src/main/resources/sensor.txt")
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() { // 该方法适合没有乱序的情况(其他方面与BoundedOutOfOrdernessTimestampExtractor一样), 也就不需要传入watermark最大等待时间
                    @Override
                    public long extractAscendingTimestamp(SensorReading sensorReading) {
                        return sensorReading.getTimestamp();
                    }
                })
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.milliseconds(300)) { // 乱序数据 设置watermark最大等待时间
//                    @Override
//                    public long extractTimestamp(SensorReading sensorReading) {
//                        // 这里需要提取毫秒单位的时间
//                        return sensorReading.getTimestamp();
//                    }
//                })
                ;
    }
}
