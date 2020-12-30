package com.study.transform;

import com.study.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputBaseStream = env.readTextFile("src/main/resources/sensor.txt");
//        env.setParallelism(1);

        inputBaseStream
                .map(new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String s) throws Exception {
                        String[] strings = s.split(",");
                        return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                    }
                })
//                .keyBy("id")
                .keyBy(SensorReading::getId)
//                .max("temperature")
                .maxBy("temperature") // 滚动聚合, 在滚动的窗口中输出该窗口温度最大的那条数据
                .print();


        env.execute();
    }

}
