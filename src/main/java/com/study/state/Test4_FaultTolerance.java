package com.study.state;

import com.study.beans.SensorReading;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用状态后端进行容错
 */
public class Test4_FaultTolerance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));
        // rocksdb需要额外引入依赖
        env.setStateBackend(new RocksDBStateBackend(""));


        SingleOutputStreamOperator<SensorReading> stream = env.readTextFile("src/main/resources/sensor.txt")
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                });


        env.execute();
    }
}
