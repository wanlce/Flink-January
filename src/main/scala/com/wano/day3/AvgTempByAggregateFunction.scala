package com.wano.day3

import com.wano.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 增量窗口聚合
 */
object AvgTempByAggregateFunction {
  def main(args: Array[String]): Unit = {

    val env =  StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new AvgTempAgg)
      .print()

    env.execute()

  }

  // 第一个泛型、 流中元素的类型
  // 第二个泛型 累加器的类型、 元组（传感器ID，来了多少条温度读数，来的温度读数的总和是多少）
  //第三个泛型 ： 增量聚合函数的输出类型，元组（传感器ID，窗口温度平均值）

  class AvgTempAgg extends AggregateFunction[SensorReading, (String, Long, Double), (String, Double)] {
    override def createAccumulator(): (String, Long, Double) = ("", 0L, 0.0)

    override def add(value: SensorReading, accumulator: (String, Long, Double)): (String, Long, Double) = {
      (value.id, accumulator._2 + 1,accumulator._3 + value.temperature)
    }

    override def getResult(accumulator: (String, Long, Double)): (String, Double) = {
      (accumulator._1, accumulator._3 / accumulator._2)
    }

    override def merge(a: (String, Long, Double), b: (String, Long, Double)): (String, Long, Double) = {
      (a._1, a._2 + b._2, a._3 + b._3)
    }
  }


}
