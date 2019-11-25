package com.paultech.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object KafkaSourceUtil {
  def getKafkaSource(parameterTool: ParameterTool): FlinkKafkaConsumer[String] = {
    val bootstrapServer = parameterTool.get("bootstrap-server", "10.180.210.187:6667")
    val sourceKafkaTopic = parameterTool.get("input-topic", "test")
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
