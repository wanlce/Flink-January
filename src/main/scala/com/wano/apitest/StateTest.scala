package com.wano.apitest

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import java.util

import com.wano.day2.{SensorReading, SensorSource}

object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.getConfig.setAutoWatermarkInterval(50)
    //    val inputPath = "/home/wanlce/IdeaProjects/flink_tutorial/src/main/resources/sensor.txt"
    //    val intputStream = env.readTextFile(inputPath)

      val inputStream = env.socketTextStream("localhost", 7777)

  //  val inputStream = env.addSource(new SensorSource)

    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 需求： 对于传感器温度值跳变 超过10度 报警
    val alertStream = dataStream
      .keyBy(_.id)
      //.flatMap( new TempChangeAlert(10.0))
      .flatMapWithState[(String,Double,Double),Double]({
        case (data:SensorReading, None) => (List.empty,Some(data.temperature))
        case (data:SensorReading,lastTemp:Some[Double]) =>{
          val diff = (data.temperature - lastTemp.get).abs
          if( diff > 10.0)
            (List((data.id,lastTemp.get,data.temperature)),Some(data.temperature))
          else
            (List.empty,Some(data.temperature))
        }
      })

    alertStream.print()

    env.execute("state test")

  }

}
class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    val lastTempDescriptor = new ValueStateDescriptor[Double](
      "lastTemp",classOf[Double])
    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)

  }
  override def flatMap(reading: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度值
    val lastTemp = lastTempState.value()
    val tempDiff = (reading.temperature - lastTemp).abs
    // 跟最新的温度求差值做比较
    if ( tempDiff > threshold) {
      out.collect((reading.id,reading.temperature,tempDiff))
  }
    //更新状态
    this.lastTempState.update(reading.temperature)
  }
}



/*
//keyed State 必须定义在RichFunction 中，因为需要运行时上下文
class MyRichMapper extends RichMapFunction[SensorReading,String]{
  var valueState: ValueState[Double] = _
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("liststate",classOf[Int]))
  lazy val mapState: MapState[String,Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String,Double]("mapstate",classOf[String],classOf[Double]))
  lazy val reduceState:ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reducestate",new MyReducer,classOf[SensorReading]))


  override def open(parameters: Configuration): Unit = {
   val valueState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("values",classOf[Double]))
  }

  override def close(): Unit = super.close()

  override def map(in: SensorReading): Unit = {
    val myV = valueState.value()
    valueState.update(in.temperature)
    listState.add(1)
    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    listState.addAll(list)
    listState.update(list)
    listState.get()

    mapState.contains("sensor_1")
    mapState.get("sensor_1")
    mapState.put("sensor_1",1.3)

  }
}*/
