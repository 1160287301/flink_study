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

/**
 * 需求: 连续10s内温度上升, 就报警
 */
public class Test2_KeyedProcessFunctionApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<SensorReading> dataStream = env.socketTextStream("localhost", 7777)
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                });

        // 测试keyedprocessfunction, 先分组然后自定义处理
        dataStream.keyBy("id")
                .process(new MyProcess(5))
                .print();


        env.execute();

    }

    private static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, String> {
        private Integer interval;
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续" + interval.toString() + "上升!");
            timerTsState.clear();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-ts", Long.class));
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
            timerTsState.clear();
        }

        public MyProcess(int i) {
            interval = i;

        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {
            // 取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();
            // 如果温度上升且没有定时器,注册10s后的定时器,开始等待
            if (sensorReading.getTemperature() > lastTemp && timerTs == null) {
                // 计算出定时器的时间戳
                long ts = context.timerService().currentProcessingTime();
                context.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            } else if (sensorReading.getTemperature() < lastTemp && timerTs != null) {
                context.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }
            lastTempState.update(sensorReading.getTemperature());
        }
    }
}
