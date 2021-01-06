package com.study.window;

import com.study.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.OutputTag;

public class Test2_OtherAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> map = env.readTextFile("src/main/resources/sensor.txt")
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                });

        // 其他可选api
        SingleOutputStreamOperator<SensorReading> sum = map.keyBy("id")
                .timeWindow(Time.seconds(15))
                .trigger(new Trigger<SensorReading, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(SensorReading sensorReading, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return null;
                    }

                    @Override
                    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

                    }
                })
                .evictor(new Evictor<SensorReading, TimeWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<SensorReading>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<SensorReading>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

                    }
                })
                .allowedLateness(Time.minutes(1))  // 比如窗口是9点关闭, 用了这个方法就会在9点输出一个近似结果,之后的一分钟内每来一条数据就更新一次结果
                .sideOutputLateData(new OutputTag<SensorReading>("late") {
                }) // 超过限制时间之后迟到的数据放在测输出流里面
                .sum(2);
        DataStream<SensorReading> sideOutput = sum.getSideOutput(new OutputTag<SensorReading>("late") {
        });// 获取测输出流里面的数据


        env.execute();
    }
}
