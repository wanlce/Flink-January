package com.wano.day1

import WordCountFromSocket.WordWithCount
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
object WordCountFromBatch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stram = env.fromElements(
      "hello world",
      "hello world",
      "hello world"
    )
    val tranformed = stram
      .flatMap(l => l.split("\\s"))
      .map(w => WordWithCount(w,1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    tranformed.print()

    env.execute("wordcount batch")

  }

}
