package com.study.state;

import com.study.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Test3_KeyedStateApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> dataStream = env.readTextFile("src/main/resources/sensor.txt")
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                });


        // 定义一个flatmap操作,检查温度的变化, 输出报警
        dataStream.keyBy("id")
                .flatMap(new TempChangeWarning(10.0))
                .print();

        env.execute();
    }

    private static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        // 温度阈值
        private Double threshold;

        public TempChangeWarning(double v) {
            this.threshold = v;
        }

        // 定义状态保存上一次的温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, 0.0));
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }

        @Override

        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            // 获取状态
            Double lastTemp = lastTempState.value();
            // 如果状态不为null,那么久判断两次温度差值
            if (lastTemp != null) {
                double diff = Math.abs(sensorReading.getTemperature() - lastTemp);
                if (diff >= threshold)
                    collector.collect(new Tuple3<>(sensorReading.getId(), lastTemp, sensorReading.getTemperature()));
            }
            // 更新状态
            lastTempState.update(sensorReading.getTemperature());

        }
    }
}
