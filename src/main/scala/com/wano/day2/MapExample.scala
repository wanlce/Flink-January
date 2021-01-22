package com.wano.day2

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object MapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
  /*  stream.map(
      r => r.id
    ).print()
*/
    stream.map(new MyMapFunction).print()

    env.execute()
    
  }
  class MyMapFunction extends MapFunction[SensorReading,String] {
    override def map(t: SensorReading): String = t.id
  }

}
