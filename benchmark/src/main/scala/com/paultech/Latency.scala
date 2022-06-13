package com.paultech

import com.paultech.util.KafkaUtil
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object Latency {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setLatencyTrackingInterval(100)

    val parameterTool = ParameterTool.fromArgs(args)
    val parallelism = parameterTool.getInt("parallelism", 12)
    env.setParallelism(parallelism)

    val kafkaSource = KafkaUtil.getKafkaSource(parameterTool)
    val kafkaSink = KafkaUtil.getKafkaSink(parameterTool)

    val dataStream = env.addSource(kafkaSource).name("kafka-source")

    dataStream.map(new MapFunction[String, String] {
      override def map(t: String): String = {
        val spaceIndex = t.indexOf(" ")
        val eventTime = t.substring(0, spaceIndex)
        val processingTime = System.currentTimeMillis().toString
        s"$eventTime $processingTime"
      }
    }).name("latency-map")
      .addSink(kafkaSink).name("kafka-sink")

    env.execute("Latency Job")
  }
}
