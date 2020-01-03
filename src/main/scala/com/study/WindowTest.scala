package com.study

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object WindowTest {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)


  def main(args: Array[String]): Unit = {
    val stream = env.readTextFile("/Users/h/PycharmProjects/flink_study/src/main/resources/sensor.txt")
    stream.print()
    env.execute("window test")
  }

}
