package com.study.processFunction;

import com.study.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Test1_keyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<SensorReading> dataStream = env.readTextFile("src/main/resources/sensor.txt")
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                });

        // 测试keyedprocessfunction, 先分组然后自定义处理
        dataStream.keyBy("id")
                .process(new MyProcess())
                .print();


        env.execute();

    }

    private static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> {
        ValueState<Long> tsTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }

        @Override
        public void close() throws Exception {
            tsTimer.clear();
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<Integer> collector) throws Exception {
            // 上下文信息
            context.timestamp();  // 获取当前数据的事件时间
            context.getCurrentKey(); // 获取当前数据重分区的key
//            context.output(output);  // 将数据输出到侧输出流
            // 定时服务
            context.timerService().currentProcessingTime();  // 获取当前处理时间
            context.timerService().currentWatermark();  // 获取当前watermark
            Long timestamp = sensorReading.getTimestamp() + 100;
            tsTimer.update(timestamp);
            context.timerService().registerProcessingTimeTimer(100);  // 注册处理时间的定时服务
//            context.timerService().registerEventTimeTimer(tsTimer.value());  // 注册事件时间的定时服务
//            context.timerService().deleteEventTimeTimer(tsTimer.value());  // 删除闹钟, 可以通过状态去关闭创建的闹钟


        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + " 定时闹钟触发");
        }
    }
}
