package com.paultech

import com.paultech.util.KafkaUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Throughput {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val parameterTool = ParameterTool.fromArgs(args)
    val parallelism = parameterTool.getInt("parallelism", 12)

    env.setParallelism(parallelism)

    val datasource = KafkaUtil.getKafkaSource(parameterTool)

    if (parameterTool.has("startFromEarliest")) {
        datasource.setStartFromEarliest()
    }

    val stream: DataStream[String] = env.addSource(datasource).name("kafka-source")

    val sink = KafkaUtil.getKafkaSink(args)

    stream.windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1))).process(new ProcessAllWindowFunction[String, String, TimeWindow] {
      override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
        out.collect(elements.size.toString)
      }
    }).name("throughput-map")
      .addSink(sink).name("kafka-sink")

    env.execute("Throughput identity Job")
  }
}
