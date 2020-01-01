package com.study

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random


case class SensorReading(id: String, timestamp: Long, temperature: Double)

class SensorSource() extends SourceFunction[SensorReading] {

  // 定义一个 flag, 表示数据源是否正常运行
  var running: Boolean = true

  // 正常生产数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    var curTemp = 1 to 10 map (
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
      )

    while (running) {
      curTemp.foreach(
        t => sourceContext.collect(SensorReading(t._1, System.currentTimeMillis(), t._2 + rand.nextGaussian()))
      )
      Thread.sleep(500)

    }
  }

  // 取消数据源的生产
  override def cancel(): Unit = {
    running = false
  }
}


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

  def fromCustom(): Unit = {
    // 自定义 source
    val stream = env.addSource(new SensorSource())
    stream.print("custom_stream")

    env.execute("source test")
  }

  def main(args: Array[String]): Unit = {
    //    fromCollection()
    //    fromKafka()
    fromCustom()
  }


}
