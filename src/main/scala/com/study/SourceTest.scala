package com.study

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def fromCollection(): Unit = {
    // 从自定义集合中读取数据
    val stream = env.fromCollection(List(
      SensorReading("sensor_1", 123, 123.123),
      SensorReading("sensor_2", 456, 456.456),
      SensorReading("sensor_3", 789, 789.789)
    ))

    stream.print("sensor_stream").setParallelism(1)

    env.execute("source test")
  }

  def fromKafka(): Unit = {
    // 从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")

    val stream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    stream.print("kafka_stream").setParallelism(1)

    env.execute("source test")
  }

  def main(args: Array[String]): Unit = {
    fromCollection()
    fromKafka()
  }

}
