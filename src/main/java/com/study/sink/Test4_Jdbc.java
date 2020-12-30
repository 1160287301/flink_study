package com.study.sink;

import com.study.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Test4_Jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/sensor.txt");
        SingleOutputStreamOperator<SensorReading> map = streamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] strings = s.split(",");
                return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
            }
        });
        map.addSink(new MyJdbcSink());

        env.execute();
    }


}

class MyJdbcSink extends RichSinkFunction<SensorReading> {
    Connection connection = null;
    PreparedStatement insertStmt = null;
    PreparedStatement updateStmt = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?useSSL=false", "root", "123456");
        insertStmt = connection.prepareStatement("insert into student (name, gender) values (?, ?)");
        updateStmt = connection.prepareStatement("update student set gender = ? where name = ?");
    }

    @Override
    public void close() throws Exception {
        insertStmt.close();
        updateStmt.close();
        connection.close();
    }

    @Override
    public void invoke(SensorReading value, Context context) throws Exception {
        insertStmt.setString(1, value.getId());
        insertStmt.setString(2, value.getTemperature().toString());
        insertStmt.execute();
    }
}