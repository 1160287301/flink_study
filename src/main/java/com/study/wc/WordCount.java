package com.study.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Flink Java API不像Scala API可以随便写lambda表达式，写完以后需要使用returns方法显式指定返回值类型，否则会报下面错误，大概意思就是说Java的lambda表达式不能提供足够的类型信息，需要指定返回值类型。不推荐使用lambda表达式而是使用匿名类。
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "src/main/resources/words.txt";
        DataSet<String> dataSource = environment.readTextFile(inputPath);
        DataSet<Tuple2<String, Integer>> sum = dataSource.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String value : s.split(" ")
                        ) {
                            collector.collect(new Tuple2(value, 1));
                        }
                    }
                })
                .groupBy(0)
                .sum(1);
        sum.print();
    }
}
