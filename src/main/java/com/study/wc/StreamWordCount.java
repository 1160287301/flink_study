package com.study.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();  // 这里的slot共享组是"default"
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        environment.disableOperatorChaining(); // 全局禁用任务链


        environment.setStateBackend(new FsStateBackend("file:///Users/h/PycharmProjects/flink_study/checkpoint"));
        environment.enableCheckpointing(300);
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);  // 设置模式, 比如精准一次, 至少一次
        environment.getCheckpointConfig().setCheckpointTimeout(60000L);  // checkpoint 超时时间
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(2);  // 设为 2 表示前一个 checkpoint 没搞完但是下一个 checkpoint 可以开始启动
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);  // 两个 checkpoint 之间的最小间隔时间
        environment.getCheckpointConfig().setPreferCheckpointForRecovery(true);  // 默认为 false, 表示不管是检查点还是保存点, 出错了就用最近的那个来恢复; true 表示出错了用最近的检查点来恢复
        environment.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);  // 能容忍的 checkpoint 错误的次数, 默认是 0 次,表示只要 checkpoint 错了就代表程序出错
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));

        // 从文件中读取数据
//        String inputPath = "src/main/resources/words.txt";
//        DataStream<String> dataSource = environment.readTextFile(inputPath);
        // 从 socket 文本流读取数据 nc -lk 7777  window(nc -L -p 7777)
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        int port = parameterTool.getInt("port");
        DataStream<String> dataSource = environment.socketTextStream("localhost", 7777);
        DataStream<Tuple2<String, Integer>> sum = dataSource.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String value : s.split(" ")
                        ) {
                            collector.collect(new Tuple2(value, 1));
                        }
                    }
                })
//                .slotSharingGroup("green") // 设置不同的slot共享组, 不同的共享组必须使用不同的slot, 没有设置的算子默认使用前面一个算子的共享组
//                .keyBy(0)
                .keyBy(item -> item.f0)
                .sum(1);
//                .slotSharingGroup("rad");  // 不是要任务链, 可以调用 disableChaining() 或者 suffer() 或者 startNewChain()
        sum.print("11111");
        environment.execute();
    }
}
