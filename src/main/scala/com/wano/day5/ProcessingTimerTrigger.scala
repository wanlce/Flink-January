package com.wano.day5

import java.sql.Timestamp

import com.wano.day2.{SensorReading, SensorSource}
import com.wano.day5.TriggerExample.WindowCount
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object ProcessingTimerTrigger {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val stream = env.addSource(new SensorSource)
      .filter( r => r.id.equals("sensor_1"))
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .trigger(new OneSecondIntervalTrigger)
      .process(new WindowCount)


    stream.print()
    env.execute()

  }
  class OneSecondIntervalTrigger extends Trigger[SensorReading, TimeWindow] {

    // 每来一条数据都要调用一次
    override def onElement(t: SensorReading, l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult = {
      // 默认为false
      // 当第一条事件来的时候，会在后面的代码中将firstSeen置为true
      val firstSeen = triggerContext.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )

      if (!firstSeen.value()) {

        // 如果单钱水位线是1234ms 那么t = 1234 + （1000 -1234 % 1000） = 2000
        val  pt = triggerContext.getCurrentProcessingTime + (1000 - (triggerContext.getCurrentProcessingTime % 1000))

        triggerContext.registerProcessingTimeTimer(pt) // 在第一条数据的时间戳之后的整数秒注册一个定时器
        triggerContext.registerProcessingTimeTimer(w.getEnd) // 在窗口结束时间注册一个定时器
        firstSeen.update(true)
      }
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult = {
      // 在onElement函数中，我们注册过窗口结束时间的定时器
      // 将窗口闭合的默认触发操作override掉了
      if (l == w.getEnd) {
        // 在窗口闭合时，触发计算并清除窗口
        TriggerResult.FIRE_AND_PURGE
      } else {
        val t = triggerContext.getCurrentProcessingTime + (1000 - (triggerContext.getCurrentProcessingTime % 1000))
        if (t < w.getEnd) {
          triggerContext.registerProcessingTimeTimer(t)
        }
        // 触发窗口计算
        println("定时器触发的时间为： " + l)
        TriggerResult.FIRE
      }
    }

    override def onEventTime(l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult = {

      TriggerResult.CONTINUE
    }

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
      // 状态变量是一个单例
      val firstSeen = triggerContext.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )
      firstSeen.clear()
    }
  }

  class WindowCount extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
      out.collect("窗口中有  " + elements.size + " 条数据！窗口结束时间是： " + context.window.getEnd)
    }
  }
}
