package com.wano.day4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
object WatermarkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream1 = env.socketTextStream("localhost", 7777, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)

    val stream2 = env.socketTextStream("localhost", 7788, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)


    stream1
      .union(stream2)
      .keyBy(_._1)
      .process(new Keyed)
      .print()

    env.execute()

  }
  class Keyed extends KeyedProcessFunction[String, (String, Long), String] {
    //每到一条数据就会调用一次
    override def processElement(i: (String, Long), context: KeyedProcessFunction[String, (String, Long), String]#Context, collector: Collector[String]): Unit = {
      collector.collect("当前的水位线是： " + context.timerService().currentWatermark())
    }
  }

}
