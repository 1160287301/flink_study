package com.study

import com.study.ProcessFunctionAPI.env
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
* 状态编程
*
* */
object StateFunctionApi {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  def bound(): Unit = {
    // 需求: 两次温度变化过大,比如超过10度就报警就报警
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
//      .process(new TempBound(10.0)) // 方法一
//        .flatMap(new TempBound1(10.0)) // 方法二
        .flatMapWithState[(String, Double, Double), Double]{ // 方法三
      // 如果没有状态, 也就是没有数据来过, 那么就将当前数据温度值存入状态
      case (input: SensorReading, None) => (List.empty, Some(input.temperature))
      // 如果有状态, 就应该与上一次的温度进行比较, 如果大于阈值就输出报警
      case (input:SensorReading, lastTemp: Some[Double]) => {
        val diff = (input.temperature - lastTemp.get).abs
        if (diff > 10.0) {
          (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
        } else {
          (List.empty, Some(input.temperature))
        }
      }
    }
    stream.print()
    processedStream.print("process temp")
    env.execute("process test")
  }

  def main(args: Array[String]): Unit = {
    bound()
  }
}

class TempBound(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {
  // 定义一个状态,用来保存上一个数据的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTempState", classOf[Double]))


  // 输出类型为(传感器id, 上一次温度, 这一次温度)
  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, collector: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp = lastTempState.value()
    // 用当前的温度值和上次的求差, 如果大于阈值, 输出报警信息
    val diff = (i.temperature - lastTemp).abs
    if (diff > threshold) {
      collector.collect((i.id, i.temperature, lastTemp))
    }
    lastTempState.update(i.temperature)
  }
}

class TempBound1(d: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  // 定义一个状态,用来保存上一个数据的温度值,只能在open中拿到值
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTempState", classOf[Double]))
  }

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp = lastTempState.value()
    // 用当前的温度值和上次的求差, 如果大于阈值, 输出报警信息
    val diff = (in.temperature - lastTemp).abs
    if (diff > d) {
      collector.collect((in.id, in.temperature, lastTemp))
    }
    lastTempState.update(in.temperature)
  }
}
