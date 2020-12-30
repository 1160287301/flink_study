package com.study.sink;

import com.study.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class Test3_ElasticSearch {
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
        // 定义es的链接配置
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));

        map.addSink((SinkFunction<SensorReading>) new ElasticsearchSink.Builder<SensorReading>(httpHosts, new ElasticsearchSinkFunction<SensorReading>() {
            @Override
            public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, String> hashMap = new HashMap<>();
                hashMap.put("id", sensorReading.getId());
                hashMap.put("temp", sensorReading.getTemperature().toString());
                hashMap.put("ts", sensorReading.getTimestamp().toString());

                // 创建请求,作为向es发起写入请求
                IndexRequest indexRequest = Requests.indexRequest()
                        .index("sensor")
                        .type("readingdata")
                        .source(hashMap);
                requestIndexer.add(indexRequest);
            }
        }));
        env.execute();
    }
}
