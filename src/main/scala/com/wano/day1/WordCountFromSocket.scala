package com.wano.day1

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCountFromSocket {

  case class WordWithCount(word: String, count: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.socketTextStream("localhost", 7777, '\n')
    val transformed = stream
      .flatMap(l => l.split("\\s"))
      .map(w => WordWithCount(w, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")

    transformed.print().setParallelism(1)

    // 执行计算逻辑
    env.execute("Word Count")


  }


}
