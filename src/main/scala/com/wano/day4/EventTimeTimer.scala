package com.wano.day4

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//a 1
//a 12
//a 23
//a 34
object EventTimeTimer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost", 7777, '\n')
      .map( line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .process(new Keyed)

    stream.print()

    env.execute()
  }
  class Keyed extends KeyedProcessFunction[String, (String, Long), String] {
    override def processElement(i: (String, Long), context: KeyedProcessFunction[String, (String, Long), String]#Context, collector: Collector[String]): Unit = {
      // 注册一个定时器， 时间携带的时间出加上10秒
      context.timerService().registerEventTimeTimer(i._2 + 10 * 1000L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect(" 定时器触发了！ " + " 时间戳是： " + new Timestamp(timestamp))
    }

  }

}
