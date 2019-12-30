package com.study

import org.apache.flink.streaming.api.scala._


case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {

  def fromCollection(): Unit = {
    // 从自定义集合中读取数据
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromCollection(List(
      SensorReading("sensor_1", 123, 123.123),
      SensorReading("sensor_2", 456, 456.456),
      SensorReading("sensor_3", 789, 789.789)
    ))

    stream.print("sensor_stream").setParallelism(1)

    env.execute("source test")
  }

  def main(args: Array[String]): Unit = {
    fromCollection()
  }

}
