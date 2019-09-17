package com.paultech

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val parameterTool = ParameterTool.fromArgs(args)

    val input = parameterTool.get("input", "hdfs:///input.txt")
    val output = parameterTool.get("output", "hdfs:///output.txt")

    val parallelism = parameterTool.getInt("parallelism", 12)

    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(parallelism)

    val source = env.readTextFile(input)

    source
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .writeAsText(output)

    env.execute("Word Count Job")
  }
}
