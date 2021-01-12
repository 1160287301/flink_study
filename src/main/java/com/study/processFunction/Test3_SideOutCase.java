package com.study.processFunction;

import com.study.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Test3_SideOutCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<SensorReading> dataStream = env.readTextFile("src/main/resources/sensor.txt")
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                });

        // 定义一个outputtag, 用来表示侧输出流 低温流
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp") {
        };

        // 测试processfunction, 自定义侧输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> hightTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
                if (sensorReading.getTemperature() > 5) {
                    collector.collect(sensorReading);
                } else {
                    context.output(lowTempTag, sensorReading);
                }
            }
        });
        hightTempStream.print("hightTempStream");
        hightTempStream.getSideOutput(lowTempTag).print("lowTempStream");


        env.execute();

    }

}
