package com.study

import akka.protobuf.DescriptorProtos.DescriptorProto.ExtensionRange
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._

/*
* 检查点的一些配置
* */
object CheckPointAPI {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置状态保存方式
    env.setStateBackend(new FsStateBackend(""))
    // 隔多长时间触发一次checkpoint
    env.enableCheckpointing(6000)
    // 设置不同的状态一致性的语义
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    // 设置checkpoint超时时间, 如果超时,该次checkpoint直接抛弃 单位毫秒
    env.getCheckpointConfig.setCheckpointTimeout(100000)
    // 设置如果checkpoint失败,fail掉当前任务
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 设置当前同时发生checkpoint的数量
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 设置两个checkpoint之间的最小时间间隔,与上面的冲突
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)
    // 开启checkpoint的外部持久化(如果一个任务fail了,该任务已经持久化的checkpoint信息会被自动清理,如果开启该选项,则需要手动清理)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    // 设置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 500))


  }

}
