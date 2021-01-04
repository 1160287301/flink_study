package com.study.window;

import com.study.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test1_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> map = env.readTextFile("src/main/resources/sensor.txt")
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                });

        // 开窗测试
        map.keyBy("id")
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)));  // 与 timewindow 方法作用一样
//                .timeWindow(Time.seconds(15));  // 有两组构造方法, 传一个参数的事滚动窗口,两个的事滑动窗口
//                .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
                .countWindow(10, 2);
        env.execute();
    }
}
