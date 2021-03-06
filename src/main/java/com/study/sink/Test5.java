package com.study.sink;

import com.study.sink.beans.ItemViewCount;
import com.study.sink.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

public class Test5 {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        DataStream<String> inputStream = env.readTextFile("/Users/h/PycharmProjects/flink_study/UserBehavior.csv");

//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("group.id", "consumer");
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset", "latest");

//        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("hotitems", new SimpleStringSchema(), properties));


        // 3. 转换为POJO，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 4. 分组开窗聚合，得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))    // 过滤pv行为
                .keyBy("itemId")    // 按商品ID分组
//                .timeWindow(Time.hours(1), Time.minutes(5))    // 开滑窗
                .timeWindow(Time.seconds(5), Time.seconds(2))    // 开滑窗
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        // 5. 收集同一窗口的所有商品count数据，排序输出top n
        SingleOutputStreamOperator<String> resultStream = windowAggStream
                .keyBy("windowEnd")    // 按照窗口分组
//                .keyBy(ItemViewCount::getWindowEnd)    // 按照窗口分组
                .process(new TopNHotItems(5));   // 用自定义处理函数排序取前5
        resultStream.print("resultStream");
        // 定义jedis连接配置
//        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
//                .setHost("localhost")
//                .setPort(6379)
//                .setDatabase(5)
//                .build();

//        resultStream.addSink(new RedisSink(config, new RedisMapper<String>() {
//            @Override
//            public RedisCommandDescription getCommandDescription() {
//                return new RedisCommandDescription(RedisCommand.SET, "SensorReading");
//            }
//
//            @Override
//            public String getKeyFromData(String map) {
//                Random a = new Random(47);
//                return "123" + a.nextInt();
//            }
//
//            @Override
//            public String getValueFromData(String map) {
//                return map;
//            }
//
//        }));

        env.execute("hot items analysis");
    }

    // 实现自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        Set a = new HashSet();


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            this.a.add(value.getTimestamp() / 1000);
            accumulator = Long.valueOf(a.size());
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

    // 实现自定义KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        // 定义属性，top n的大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，存入List中，并注册定时器
            itemViewCountListState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });
            StringBuilder resultBuilder = new StringBuilder();
            // 遍历列表，取top n输出
            for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append(currentItemViewCount.getWindowEnd() + "-" + currentItemViewCount.getItemId() + ":" + currentItemViewCount.getCount() + ";");
            }
            out.collect(resultBuilder.toString());
            // 控制输出频率
//            Thread.sleep(5000L);
        }
    }


    //resultStream:2> 1511658011000-88:1;1511658011000-66:1;1511658011000-99:1;
    //resultStream:7> 1511658003000-22:1;1511658003000-11:1;
    //resultStream:6> 1511658005000-44:1;1511658005000-22:1;1511658005000-33:1;1511658005000-11:1;
    //resultStream:7> 1511658009000-44:1;1511658009000-88:1;1511658009000-55:1;1511658009000-66:1;1511658009000-99:1;
    //resultStream:5> 1511658007000-44:1;1511658007000-55:1;1511658007000-22:1;1511658007000-33:1;1511658007000-66:1;
    //resultStream:2> 1511658013000-99:1;


    //resultStream> 1511658003000-11:1;1511658003000-22:1;
    //resultStream> 1511658005000-22:1;1511658005000-11:1;1511658005000-44:1;1511658005000-33:1;
    //resultStream> 1511658007000-66:1;1511658007000-44:1;1511658007000-33:1;1511658007000-55:1;1511658007000-22:1;
    //resultStream> 1511658009000-66:1;1511658009000-44:1;1511658009000-99:1;1511658009000-88:1;1511658009000-55:1;
    //resultStream> 1511658011000-66:1;1511658011000-55:1;1511658011000-99:1;1511658011000-88:1;
    //resultStream> 1511658013000-99:1;
}
