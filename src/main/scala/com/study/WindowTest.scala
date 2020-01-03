package com.study

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.readTextFile("%s\\src\\main\\resources\\sensor.txt".format(System.getProperty("user.dir")))
    val dataStream = stream.map(
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )
      //      .assignAscendingTimestamps(_.timestamp * 1000) // 针对无乱序的数据
      //      .assignTimestampsAndWatermarks(new MtAssigner()) // 针对乱序的数据
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
    })
    //    env.getConfig.setAutoWatermarkInterval(100L)  // 设置生成watermark的时间 默认200毫秒
    val minTempPerWindow = dataStream.map(data => (data.id, data.temperature))
      .keyBy(0)
      // 统计 15 秒内每个传感器的最低温度 滚动窗口
      //      .timeWindow(Time.seconds(15))
      // 统计 15 秒内的最小温度, 隔5秒输出一次, 滑动窗口
      //      .timeWindow(Time.seconds(15), Time.seconds(5))
      // 底层的调用方式,该方式可以传入时区
      .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5), Time.hours(-8)))
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    minTempPerWindow.print("min temp") // 默认的时间是 processor 的时间, 如果程序没有运行 10 秒,则什么都没有输出
    env.execute("window test")
  }

}

class MtAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound = 6000
  var maxTs = Long.MinValue

  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTs = maxTs.max(t.timestamp * 1000)
    t.timestamp * 1000
  }

}