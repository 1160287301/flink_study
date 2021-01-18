package com.study.state;

import com.study.beans.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用状态后端进行容错
 */
public class Test4_FaultTolerance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));
        // rocksdb需要额外引入依赖
        env.setStateBackend(new RocksDBStateBackend(""));

        // 2.检查点配置
        env.enableCheckpointing(300);  // 启用 checkpoint, 默认是 500 毫秒进行一次 checkpoint
        // 高级选项
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);  // 设置模式, 比如精准一次, 至少一次
        env.getCheckpointConfig().setCheckpointTimeout(60000L);  // checkpoint 超时时间
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);  // 设为 2 表示前一个 checkpoint 没搞完但是下一个 checkpoint 可以开始启动
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);  // 两个 checkpoint 之间的最小间隔时间
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);  // 默认为 false, 表示不管是检查点还是保存点, 出错了就用最近的那个来恢复; true 表示出错了用最近的检查点来恢复
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);  // 能容忍的 checkpoint 错误的次数, 默认是 0 次,表示只要 checkpoint 错了就代表程序出错

        // 3.重启策略配置
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));  // 固定时间间隔重启; 重启三次, 每次间隔 10s
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.minutes(10), Time.minutes(1)));  // 失败率重启; 10 分钟之内重启三次, 每次间隔一分钟


        SingleOutputStreamOperator<SensorReading> stream = env.readTextFile("src/main/resources/sensor.txt")
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                });


        env.execute();
    }
}
