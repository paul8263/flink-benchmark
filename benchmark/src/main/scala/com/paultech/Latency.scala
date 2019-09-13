package com.paultech

import com.paultech.util.{KafkaSinkUtil, KafkaSourceUtil}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object Latency {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val parameterTool = ParameterTool.fromArgs(args)
    val parallelism = parameterTool.getInt("parallelism", 12)
    env.setParallelism(parallelism)

    val kafkaSource = KafkaSourceUtil.getKafkaSource(args)

    val dataStream = env.addSource(kafkaSource)

    dataStream.map(new MapFunction[String, String] {
      override def map(t: String): String = {
        val eventTime = t.split(" ")(0)
        val processingTime = System.currentTimeMillis().toString
        s"$eventTime $processingTime"
      }
    })

    val kafkaSink = KafkaSinkUtil.getKafkaSink(args)

    dataStream.addSink(kafkaSink)

    env.execute("Latency Job")
  }
}
