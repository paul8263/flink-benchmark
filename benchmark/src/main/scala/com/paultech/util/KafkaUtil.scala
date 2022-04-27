package com.paultech.util

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import java.util.Properties

object KafkaUtil {
  def getKafkaSink(args: Array[String]): FlinkKafkaProducer[String] = {
    val parameterTool = ParameterTool.fromArgs(args)
    val bootstrapServer = parameterTool.get("bootstrap-server", "localhost:9092")
    val outputTopic = parameterTool.get("output-topic", "output-topic")

    new FlinkKafkaProducer[String](bootstrapServer, outputTopic, new SimpleStringSchema())
  }

  def getKafkaSource(parameterTool: ParameterTool): FlinkKafkaConsumer[String] = {
    val bootstrapServer = parameterTool.get("bootstrap-server", "localhost:9092")
    val sourceKafkaTopic = parameterTool.get("input-topic", "input-topic")
    val pollTimeout = parameterTool.get("flink.poll-timeout", "")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServer)
    properties.setProperty("group.id", "flink-bench")
    if (!"".equals(pollTimeout)) {
      properties.setProperty("flink.poll-timeout", pollTimeout)
    }

    new FlinkKafkaConsumer[String](sourceKafkaTopic, new SimpleStringSchema(), properties)
  }
}
