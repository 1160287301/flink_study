package com.study.source;

import com.study.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

class MySensorSource implements SourceFunction<SensorReading> {
    private Boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        Random random = new Random(47);
        while (running) {
            sourceContext.collect(new SensorReading("senor_" + random.nextInt(), System.currentTimeMillis(), random.nextDouble()));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}


public class SourceTest4_Consumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> streamSource = env.addSource(new MySensorSource());
        streamSource.print();
        env.execute();
    }
}
