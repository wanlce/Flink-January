package com.wano.day2

import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoFlatMapExample {
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

    val printed = connected.flatMap(new MyCoFlatMapFunction)
    printed.print()

    env.execute()

  }

  class MyCoFlatMapFunction extends CoFlatMapFunction[(String,Int),(String,Int), String] {
    override def flatMap1(in1: (String, Int), collector: Collector[String]): Unit = {
      collector.collect(in1._1 + "的体重是：" + in1._2 + "斤")
      collector.collect(in1._1 + "的体重是：" + in1._2 + "斤")

    }

    override def flatMap2(in2: (String, Int), collector: Collector[String]): Unit = {

      collector.collect(in2._1 + "的年龄是：" + in2._2 + "岁")

    }
  }
}
