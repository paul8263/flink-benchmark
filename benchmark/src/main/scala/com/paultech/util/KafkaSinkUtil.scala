package com.paultech.util

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaSinkUtil {
  def getKafkaSink(args: Array[String]): FlinkKafkaProducer[String] = {
    val parameterTool = ParameterTool.fromArgs(args)
    val bootstrapServer = parameterTool.get("bootstrap-server", "10.180.210.187:6667")
    val outputTopic = parameterTool.get("output-topic", "output")

    new FlinkKafkaProducer[String](bootstrapServer, outputTopic, new SimpleStringSchema())
  }
}
