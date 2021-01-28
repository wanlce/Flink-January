package com.wano.day4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RedirectLateEventCustom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 7777, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)
      .process( new LateEventProc)
    stream.print()
    stream.getSideOutput(new OutputTag[String]("late")).print()

    env.execute()

  }
class LateEventProc extends ProcessFunction[(String, Long), (String, Long)] {
  val late = new OutputTag[String]("late")
  override def processElement(i: (String, Long), context: ProcessFunction[(String, Long), (String, Long)]#Context, collector: Collector[(String, Long)]): Unit = {
    if (i._2 < context.timerService().currentWatermark()) {
      context.output(late," 迟到的数据来了")
    } else {
      collector.collect(i)
    }
  }
}
}