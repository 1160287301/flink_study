package com.study.window;

import com.study.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Test1_TimeWindow {
    /**
     * 全窗口函数, 记录一个窗口里面的所有数据, 适用于计算平均数之类
     * 这里只想统计数据的个数
     */
    public void apply() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> map = env.readTextFile("src/main/resources/sensor.txt")
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                });

        // 开窗测试
        map.keyBy("id")
                .timeWindow(Time.seconds(15))
                // process 和 apply差不多, 但是apply拿到的是window对象, process拿到的是context上下文对象, 包含window
//                .process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
//                    @Override
//                    public void process(Tuple tuple, Context context, Iterable<SensorReading> iterable, Collector<Object> collector) throws Exception {
//
//                    }
//                })
                .apply(new WindowFunction<SensorReading, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<String> collector) throws Exception {
                        System.out.println(tuple);
                        String id = tuple.getField(0);
                        int size = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(id + timeWindow.getEnd() + size);
                    }
                }).print();
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        new Test1_TimeWindow().apply();
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
//                .timeWindow(Time.seconds(15))  // 有两组构造方法, 传一个参数的事滚动窗口,两个的事滑动窗口
//                .window(EventTimeSessionWindows.withGap(Time.minutes(1)));  // 创建一个session window
                .countWindow(10, 2)
                .apply(new WindowFunction<SensorReading, Object, Tuple, GlobalWindow>() {
                    @Override
                    public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<SensorReading> iterable, Collector<Object> collector) throws Exception {
                        System.out.println(tuple);
                        String id = tuple.getField(0);
                        int size = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(id + "" + size);
                    }
                }).print();


        // 增量聚合函数 只记录累加的状态
//                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
//                    @Override
//                    public Integer createAccumulator() {
//                        return 0;
//                    }
//
//                    @Override
//                    public Integer add(SensorReading sensorReading, Integer integer) {
//                        return integer + 1;
//                    }
//
//                    @Override
//                    public Integer getResult(Integer integer) {
//                        return integer;
//                    }
//
//                    @Override
//                    public Integer merge(Integer integer, Integer acc1) {
//                        return integer + acc1;
//                    }
//                }).print();
        env.execute();
    }
}
