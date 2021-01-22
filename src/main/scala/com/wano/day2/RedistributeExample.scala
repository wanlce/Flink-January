package com.wano.day2

import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

object RedistributeExample {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource( new SensorSource)
      .setParallelism(1)
      .map(_.id).setParallelism(1)

    stream.print()

    env.execute()

  }

}
