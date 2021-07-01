package com.study.sink;

import com.study.sink.beans.ItemViewCount;
import com.study.sink.beans.UserBehavior;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class Gjh {
    public static class ItemCountAgg1 extends RichAggregateFunction<UserBehavior, Long, Long> {
        //定义状态
        ValueState<String> lastVisitDateState = null;
        //定义日期工具类
        SimpleDateFormat sdf = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            sdf = new SimpleDateFormat("yyyyMMdd");
            //初始化状态
            ValueStateDescriptor<String> lastVisitDateStateDes =
                    new ValueStateDescriptor<>("lastVisitDateState", String.class);
            //因为我们统计的是日活DAU，所以状态数据只在当天有效 ，过了一天就可以失效掉
            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.seconds(1)).build();
            lastVisitDateStateDes.enableTimeToLive(stateTtlConfig);
            this.lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDes);
        }

        @Override
        public Long createAccumulator() {
            return null;
        }

        @SneakyThrows
        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            //获取当前访问时间
            Long ts = userBehavior.getTimestamp();
            //将当前访问时间戳转换为日期字符串
            String logDate = ts.toString();
            //获取状态日期
            String lastVisitDate = lastVisitDateState.value();

            //用当前页面的访问时间和状态时间进行对比
            if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                System.out.println("已访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
            } else {
                aLong += 1;
                lastVisitDateState.update(logDate);
            }
            return aLong;
        }

        @Override
        public Long getResult(Long aLong) {
            return null;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            accumulator += 1;
            return accumulator;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> inputStream = env.readTextFile("/Users/h/PycharmProjects/flink_study/UserBehavior.csv");
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))// 过滤pv行为
                .keyBy("itemId")
                .filter(new RichFilterFunction<UserBehavior>() {
                    //定义状态
                    MapState<String, String> mapState = null;
                    //定义日期工具类
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化日期工具类
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        //初始化状态
                        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("mapState", String.class, String.class);
                        this.mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public boolean filter(UserBehavior userBehavior) throws Exception {
                        //获取当前访问时间
                        Long ts = userBehavior.getTimestamp();
                        String logDate = ts.toString();
                        //获取状态日期
                        String lastVisitDate = mapState.get(logDate);
                        System.out.println("lastVisitDate: " + mapState.keys());
                        mapState.remove(String.valueOf(ts - 1));
                        System.out.println("lastVisitDate: " + mapState.keys());
                        //用当前页面的访问时间和状态时间进行对比
                        if (StringUtils.isNotBlank(lastVisitDate)) {
                            System.out.println("已访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                            return false;
                        } else {
                            System.out.println("未访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                            mapState.put(logDate, "1");
                            return true;
                        }
                    }
                })
                .keyBy("itemId")
                .timeWindow(Time.seconds(5), Time.seconds(2))
//                .aggregate(new Test5.ItemCountAgg(), new Test5.WindowItemCountResult());
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        SingleOutputStreamOperator<String> resultStream = windowAggStream
                .keyBy("windowEnd")    // 按照窗口分组
                .process(new Test5.TopNHotItems(5));

        resultStream.print("resultStream");
        env.execute();
    }
}
