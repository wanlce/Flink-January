package com.wano.day2

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object FilterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream.filter( r => r.id.equals("sensor_1")).print()
    stream.filter( new FilterFunction[SensorReading] {
      override def filter(t: SensorReading): Boolean = t.id.equals("sensor_1")
    })


    stream.filter(new MyFilterFunction).print()

    env.execute()
  }
  class MyFilterFunction extends FilterFunction[SensorReading] {
    override def filter(t: SensorReading): Boolean = t.id.equals("sensor_1")
  }

}
