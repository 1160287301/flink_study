package com.study.udf;

import com.study.beans.SensorReading;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义重分区
 */
public class PartitionComsum {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        SingleOutputStreamOperator<SensorReading> map = env.readTextFile("src/main/resources/sensor.txt")
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                });

//        map.print("map");
        // shuffle, 完全随机分区
//        map.shuffle().print("shuffle");
        // global 全部发往第一个分区
//        map.global().print("global");
        // 自定义分区 sensor_1 发往 1 区; sensor_2 发往 2 区
        map.partitionCustom(new MyPartition(), "id").print("partitionCustom");
        env.execute();
    }
}

class MyPartition implements Partitioner<String> {
    @Override
    public int partition(String s, int i) {
        return Integer.parseInt(s.split("_")[1].trim()) - 1;
    }
}


