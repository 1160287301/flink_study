package com.study

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

case class SensorReading(id: String, timestamp: Long, temperature: Double)

class MyRedisMapper() extends RedisMapper[SensorReading] {
  // 定义保存数据到 redis 的命令
  override def getCommandDescription: RedisCommandDescription = {
    // 把数据 id 和温度值保存成哈希表 HSET key field value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  // 定义保存到 redis 的 value
  override def getKeyFromData(t: SensorReading): String = t.temperature.toString

  // 定义保存到 redis 的 key
  override def getValueFromData(t: SensorReading): String = t.id
}

class MyJdbcSink() extends RichSinkFunction[SensorReading] {
  // 定义 sql 连接, 预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updataStmt: PreparedStatement = _

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updataStmt.setDouble(1, value.temperature)
    updataStmt.setString(2, value.id)
    updataStmt.execute()
    if (updataStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  // 初始化, 创建连接和与便于语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("INSERT INTO temperature (sensor, temp) VALUES (?, ?)")
    updataStmt = conn.prepareStatement("UPDATE temperature SET temp = ? WHERE sensor = ?")
  }

  override def close(): Unit = {
    insertStmt.close()
    updataStmt.close()
    conn.close()
  }
}

object SinkTest {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  def sinkToKafka(): Unit = {
    // 输出到 kafka
    val inoutStream = env.readTextFile("xxx.txt")

    val dataStream = inoutStream.map(
      data => {
        val dataArray = data.split(",")
        // 转成 string 方便序列化
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
      }
    )

    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "sinkTest", new SimpleStringSchema()))
    env.execute("kafka sink test")
  }

  def sinkToRedis(): Unit = {
    // 输出到 redis
    val inoutStream = env.readTextFile("xxx.txt")

    val dataStream = inoutStream.map(
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    dataStream.addSink(new MyJdbcSink())
    env.execute("redis sink test")
  }

  def sinkToJdbc(): Unit = {
    // 输出到 mysql
    val inoutStream = env.readTextFile("xxx.txt")

    val dataStream = inoutStream.map(
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    dataStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper()))
    env.execute("redis sink test")
  }

  def main(args: Array[String]): Unit = {
    sinkToKafka()
    sinkToRedis()
    sinkToJdbc()
  }
}
