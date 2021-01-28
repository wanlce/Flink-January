package com.wano.day4

import com.wano.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoProcessFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env.addSource(new SensorSource)

    val switches = env.fromElements(
      ("sensor_2", 10 * 1000L)
    )
    val result = readings
      .connect(switches)
      .keyBy(_.id,_._1)
      .process(new ReadingFilter)

    result.print()
    env.execute()
  }
class ReadingFilter extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {
  lazy val forwardingEnabled = getRuntimeContext.getState(
    new ValueStateDescriptor[Boolean]("switch", Types.of[Boolean])
  )
  override def processElement1(in1: SensorReading, context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    // 如果开关是true 就允许数据流向下发送
    if (forwardingEnabled.value()) {
      collector.collect(in1)
    }
  }
  override def processElement2(in2: (String, Long), context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    // 打开开关
    forwardingEnabled.update(true)
    val ts = context.timerService().currentProcessingTime() + in2._2
    context.timerService().registerProcessingTimeTimer(ts)
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit ={
    forwardingEnabled.clear()
  }
}
}
