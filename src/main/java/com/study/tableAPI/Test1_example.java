package com.study.tableAPI;

import com.study.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Test1_example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> stream = env.readTextFile("src/main/resources/sensor.txt")
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                });

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 基于流创建一张表
        Table table = tableEnv.fromDataStream(stream);
        // 调用 table api 进行转换操作
        Table resutl1 = table.select("id,temperature")
                .where("id='sensor_4'");
        // 执行 sql
        tableEnv.createTemporaryView("sensor", table);
        Table result2 = tableEnv.sqlQuery("select id,temperature from sensor where id <> 'sensor_4'");
        tableEnv.toAppendStream(resutl1, Row.class).print("resutl1");
        tableEnv.toAppendStream(result2, Row.class).print("result2");


        env.execute();
    }
}
