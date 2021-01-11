package com.study.state;

import com.study.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 1.键控状态是根据输入数据流中定义的 key 来维护和访问的
 * 2.flink 为每个 key 维护一个状态实例,并将具有相同键的所有数据,都分区到同一个算子任务中,这个任务会维护和处理这个 key 对应的状态
 * 3.当任务处理一条数据时,它会自动将状态的访问范围限定为当前数据的 key
 */
public class Test2_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> stream = env.readTextFile("src/main/resources/sensor.txt")
                .map(line -> {
                    String[] strings = line.split(",");
                    return new SensorReading(strings[0].trim(), Long.valueOf(strings[1].trim()), Double.valueOf(strings[2].trim()));
                });
        // 定义一个有状态的map操作,统计当前不同 key 的数据个数
        stream.keyBy("id")
                .map(new MyKeyCountMapper(){})
                .print();

        env.execute();

    }

}
class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {
    private ValueState<Integer> keyCountState ;
    // 其他类型状态的生命
    private ListState<String> myListState;
    private MapState<String, Double> myMapState;
    private ReducingState myReduceState;

    @Override
    public void open(Configuration parameters) throws Exception {
        keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class, 0));
        myListState= getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
        myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map",String.class,Double.class));
        myReduceState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("my-reduce", new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                return new SensorReading(sensorReading.getId(), t1.getTimestamp(), Math.max(sensorReading.getTemperature(), t1.getTemperature()));
            }
        }, SensorReading.class));
    }

    @Override
    public Integer map(SensorReading sensorReading) throws Exception {
        Integer count = keyCountState.value();
        count++;
        keyCountState.update(count);

        // 其他状态 api 调用
        // list 取值
        Iterable<String> strings = myListState.get();
        for (String str:myListState.get()) {
            System.out.println(str);
        }
        // list 存入
        myListState.add("hello");
        // map
        myMapState.get("test");
        myMapState.put("test", 1.0);
        // reduce
        myReduceState.get();
        myReduceState.add(sensorReading);
        // 情况状态
    myReduceState.clear();
        return count;
    }
}