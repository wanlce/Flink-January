package com.wano.day2

import org.apache.flink.streaming.api.scala._

object CounsumeFromSensorSource {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream.print()

    env.execute("sensor ")
  }

}
