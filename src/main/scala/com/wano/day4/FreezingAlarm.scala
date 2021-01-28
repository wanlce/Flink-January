package com.wano.day4

import com.wano.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FreezingAlarm {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)
      .process(new FreezingAlarmFunction)

    stream.print() // 打印常规输出
    // 侧输出标签的名字必须是一样的
    stream.getSideOutput(new OutputTag[String]("freezing-alarm")).print()
    env.execute()


  }
  // ProcessFunction 处理的是没有keyby的流
  class FreezingAlarmFunction extends ProcessFunction[SensorReading, SensorReading] {
    // 定义一个侧输出标签，实际上就是侧输出流的名字
    lazy  val freezingAlarmOut = new OutputTag[String]("freezing-alarm")
    override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
      if (i.temperature < 32.0) {
        // 第一个参数是侧输出标签，第二个参数是发送的数据
        context.output(freezingAlarmOut, s"${i.id}的传感器低温报警！")
      }
      // 将所有读数发送到常规输出
      collector.collect(i)
    }

  }

}
