package com.study

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
底层 API
Flink 提供了 8 个 process function
ProcessFunction
KeyedProcessFunction
CoProcessFunction
ProcessJoinFunction
BroadcastProcessFunction
KeyedBroadcastProcessFunction
ProcessWindowFunction
ProcessAllWindowFunction
 */
object ProcessFunctionAPI {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  def timers(): Unit = {
    // TimerServer 和 定时器(Timers)
    // 需求: 某个传感器连续两次温度上升就报警

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.readTextFile("%s\\src\\main\\resources\\sensor.txt".format(System.getProperty("user.dir")))
    val dataStream = stream.map(
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })

    val processedStream = dataStream.keyBy(_.id)
      .process(new TempIncreAlert())

    stream.print()
    processedStream.print("process temp")
    env.execute("process test")
  }

  def side_outputs(): Unit = {
    // Emitting to Side Outputs(侧输出流)
    // 需求: 将传感器数据流分流, 如果小于冰点, 输出信息到侧输出流

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.readTextFile("%s\\src\\main\\resources\\sensor.txt".format(System.getProperty("user.dir")))
    val dataStream = stream.map(
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })

    val processedStream = dataStream
      .process(new FreezingAlert())

    processedStream.print("process temp")
    // 获取侧输出流并输出
    processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print()
    env.execute("process test")

  }

  def main(args: Array[String]): Unit = {
    timers()
    side_outputs()
  }


}

class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String] {

  // 定义一个状态,用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  // 定义一个状态,用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

  // 流中的每条数据都会调用该方法
  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    // 先取出上一个温度值
    val perTemp = lastTemp.value()
    // 更新温度值
    lastTemp.update(i.temperature)

    // 温度上升且没有设置过定时器,就注册定时器
    if (i.temperature > perTemp && currentTimer.value() == 0) {
      val timerTs = context.timerService().currentProcessingTime() + 1000L
      context.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    } else if (perTemp > i.temperature || perTemp == 0.0) {
      // 如果温度下降,或是第一条数据,则删除定时器并清空状态
      context.timerService().deleteEventTimeTimer(currentTimer.value())
      currentTimer.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 输出报警信息
    out.collect(ctx.getCurrentKey + "温度连续上升")
    currentTimer.clear()
  }
}

class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading]{
  lazy val alertOutput: OutputTag[String] = new OutputTag[String]("freezing alert")

  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if (i.temperature < 0){
      // 输出到测输出流
      context.output(alertOutput, "freezing alert for " + i.id)
    } else {
      // 输出到主输出流
      collector.collect(i)
    }

  }
}
