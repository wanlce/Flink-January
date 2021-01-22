package com.wano.day2

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object CoMapExample {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1 = env.fromElements(
      ("zuoyuan",120),
      ("baoyuan",100)
    )

    val stream2 = env.fromElements(
      ("zuoyuan",23),
      ("baiyuan",33)
    )

    val connected = stream1
      .keyBy(_._1)
      .connect(stream2.keyBy(_._1))

    val printed = connected.map(new MyCoMapFunction)
    printed.print()

    env.execute()

  }

  class MyCoMapFunction extends CoMapFunction[(String,Int),(String,Int), String] {
    override def map1(in1: (String, Int)): String = {
      in1._1 + "的体重是：" +in1._2 + "斤"
    }

    override def map2(in2: (String, Int)): String = {
      in2._1 + "的年龄是：" + in2._2 + "岁"
    }
  }
}
