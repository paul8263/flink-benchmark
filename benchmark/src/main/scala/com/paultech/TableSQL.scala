package com.paultech

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource
import org.slf4j.LoggerFactory

object TableSQL {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(TableSQL.getClass)
    val parameterTool = ParameterTool.fromArgs(args)

    val input = parameterTool.get("input", "hdfs:///input.csv")
    val output = parameterTool.get("output", "hdfs:///output.csv")

    val parallelism = parameterTool.getInt("parallelism", 12)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    env.setParallelism(parallelism)

    val source = CsvTableSource
      .builder()
      .path(input)
      .fieldDelimiter(" ")
      .ignoreFirstLine()
      .field("name", TypeInformation.of(classOf[String]))
      .field("age", TypeInformation.of(classOf[Int])).build()
    tableEnv.registerTableSource("input", source)

    val sink = new CsvTableSink(output, fieldDelim = " ")
    tableEnv.registerTableSink("csvSink", Array("age_avg"), Array(TypeInformation.of(classOf[Int])), sink)

    val table = tableEnv.sqlQuery("select avg(age) from input")
    tableEnv.toDataSet[Int](table).print()
    table.insertInto("csvSink")

    env.execute("Table SQL Job")
    logger.info("Table SQL Job executed")
  }
}
