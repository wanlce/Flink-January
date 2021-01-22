package com.wano.day2

import org.apache.flink.streaming.api.scala._

object KeyedStreamExample {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)
      .filter(_.id.equals("sensor_1"))

    val keyed: KeyedStream[SensorReading, String] = stream.keyBy(_.id)

    val min: DataStream[SensorReading] = keyed.min(2)

    val red: DataStream[SensorReading] = keyed.reduce((r1, r2) => SensorReading(r1.id, 0L, r1.temperature.min(r2.temperature)))

    env.execute()

  }
}