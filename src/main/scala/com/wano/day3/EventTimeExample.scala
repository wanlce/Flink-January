package com.wano.day3

import java.sql.Timestamp

import com.wano.day2.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object EventTimeExample {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost",7777,'\n')
      .map( line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      // 分配时间戳和水位线一定要在keyby 之前
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          // 告诉系统，时间戳是元组的第二个字段
          override def extractTimestamp(t: (String, Long)): Long = t._2
        }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process( new WindowResult)

    stream.print()

    env.execute()

  }
  class WindowResult extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(new Timestamp(context.window.getStart) + " ~ " + new Timestamp(context.window.getEnd) + " 的窗口中有" + elements.size + " 个元素！")
    }
  }

}
