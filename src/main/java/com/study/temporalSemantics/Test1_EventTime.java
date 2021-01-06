package com.study.temporalSemantics;

import com.study.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class Test1_EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间语义为事件发生时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(300);  // 周期生成watermark的时间周期,单位毫秒, 默认是200毫秒

        SingleOutputStreamOperator<SensorReading> watermarks = env.socketTextStream("localhost", 7777)
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                })
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() { // 该方法适合没有乱序的情况(其他方面与BoundedOutOfOrdernessTimestampExtractor一样), 也就不需要传入watermark最大等待时间
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading sensorReading) {
//                        return sensorReading.getTimestamp();
//                    }
//                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.milliseconds(300)) { // 乱序数据 设置watermark最大等待时间
                    @Override
                    public long extractTimestamp(SensorReading sensorReading) {
                        // 这里需要提取毫秒单位的时间
                        return sensorReading.getTimestamp();
                    }
                });

        // 定义一个侧输出流标签
        OutputTag<SensorReading> test1 = new OutputTag<SensorReading>("test1") {
        };

        // 基于事件时间的开窗集合, 统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minBy = watermarks.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1)) // 到了15秒會输出一个结果, 然后再之后的一分钟内来一个数据就更新一下结果
                .sideOutputLateData(test1)  // 之后迟到的数据进入侧输出流
                .minBy("temperature");

        // 获取侧输出流
        minBy.getSideOutput(test1).print("getSideOutput");

        minBy.print("minTemp");


        env.execute();
    }
}
