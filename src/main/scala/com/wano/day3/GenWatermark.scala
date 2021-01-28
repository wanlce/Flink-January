package com.wano.day3

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter
import org.apache.flink.util.Collector

object GenWatermark {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 系统默认每隔200ms插入一次水位线
    // 设置为每隔一分钟插入一次
    env.getConfig.setAutoWatermarkInterval(60000)

    val stream = env.socketTextStream("localhost",7777,'\n')
      .map( line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      // 分配时间戳和水位线一定要在keyby 之前
      .assignTimestampsAndWatermarks(
        new MyAssignear
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process( new WindowResult)

    stream.print()

    env.execute()

  }
  // 自定义实现周期型的插入水位线
  class MyAssignear extends AssignerWithPeriodicWatermarks[(String, Long)] {
      // 设置最大延迟时间
    val bound: Long = 10 * 1000L
    var maxTs: Long = Long.MinValue + bound

  // 定义抽取时间戳的逻辑，每到一个事件就调用一次
    override def extractTimestamp(t: (String, Long), l: Long): Long = {
      maxTs = maxTs.max(t._2)  // 更新观察到的最大时间戳
      t._2  //将抽取的时间戳返回
    }
    // 产生水位线的逻辑
    // 默认每隔200ms 调用一次
    // 设置了每隔一分钟调用一次
    override def getCurrentWatermark: Watermark = {
      // 观察到的最大事件事件 -  最大延迟时间
      println("观察到的最大时间戳是： " + maxTs)
      new Watermark(maxTs - bound)
    }
  }

  class WindowResult extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(new Timestamp(context.window.getStart) + " ~ " + new Timestamp(context.window.getEnd) + " 的窗口中有" + elements.size + " 个元素！")
    }
  }

}