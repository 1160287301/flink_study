package com.study

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.readTextFile("/Users/h/PycharmProjects/flink_study/src/main/resources/sensor.txt")
    val dataStream = stream.map(
      data => {
        val dataArray = data.split(",")
        // 转成 string 方便序列化
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    ).assignAscendingTimestamps(_.timestamp * 1000)
    // 统计 5 秒内每个传感器的最低温度
    val minTempPerWindow = dataStream.map(data => (data.id, data.temperature))
        .keyBy(0)
        .timeWindow(Time.seconds(5))
        .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    minTempPerWindow.print("min temp")  // 默认的时间是 processor 的时间, 如果程序没有运行 5 秒,则什么都没有输出
    env.execute("window test")
  }

}
