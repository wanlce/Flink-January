package com.wano.day5

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object IntervalJoinExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val clickStream = env.fromElements(
      ("1","click", 3600 * 1000L)
    )
    val browseStream = env.fromElements(
      ("1","browse", 2000 * 1000L),
      ("1","browse", 3100 * 1000L),
      ("1","browse", 3200 * 1000L)
    )


  }

}
