package com.study.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();  // 这里的slot共享组是"default"

        // 从文件中读取数据
        String inputPath = "C:\\myApp\\PycharmProjects\\flink_study\\src\\main\\resources\\words.txt";
        DataStream<String> dataSource = environment.readTextFile(inputPath);
        DataStream<Tuple2<String, Integer>> sum = dataSource.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String value : s.split(" ")
                        ) {
                            collector.collect(new Tuple2(value, 1));
                        }
                    }
                }).slotSharingGroup("green") // 设置不同的slot共享组, 不同的共享组必须使用不同的slot, 没有设置的算子默认使用前面一个算子的共享组
                .keyBy(0)
                .sum(1).slotSharingGroup("rad");
        sum.print();
        environment.execute();
    }
}
