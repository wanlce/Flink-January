package com.wano.day2

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random


class SensorSource extends RichParallelSourceFunction[SensorReading] {
  var runing: Boolean = true

  override def run(ctx: SourceContext[SensorReading]): Unit = {

    val rand = new Random
    var curFTemp = (1 to 10).map(
      i => ("sensor_" + i, (rand.nextGaussian() * 20))
    )
    //产生无限数据流
    while (runing) {
      curFTemp = curFTemp.map(
        t => (t._1,t._2 + (rand.nextGaussian() * 0.5))
      )
      //产生ms为单位的时间戳
      val curTime = Calendar.getInstance().getTimeInMillis

      curFTemp.foreach( t=> ctx.collect(SensorReading(t._1,curTime,t._2)))
      Thread.sleep(100)
    }
  }
  override def cancel(): Unit = runing = false
}
