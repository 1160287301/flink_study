package com.study.tableAPI;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class Test3_createTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 表的创建: 连接外部系统, 读取数据
        // 1.读取文件
        String filePath = "/Users/h/PycharmProjects/flink_study/src/main/resources/sensor.txt";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("sensor");
        Table sensor = tableEnv.from("sensor");
        // 简单转换
        Table id = sensor.select("id")
                .filter("id = 'sensor_4'");
        tableEnv.toAppendStream(id, Row.class).print("result2");
        env.execute();
    }
}
