package com.study.udf;

import com.study.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("src/main/resources/sensor.txt");
        SingleOutputStreamOperator<SensorReading> map = source.map(line -> {
            String[] strings = line.split(",");
            return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
        });


    }

    // 实现自定义富函数类
    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作, 一般是定义状态或者建立数据库连接, 每个分区都会调用一次该方法, close 方法也是一样
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            // 一般是关闭连接和清空状态的收尾操作
            super.close();
        }

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }
    }
}
