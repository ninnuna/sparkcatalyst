package com.ttn.de.context

import com.ttn.de.config.parser.JsonConfigParser.{Config, ConfigFile, SourceOrDestinationConfig, StepConfig}
import com.ttn.de.reader.S3DataReader
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object StepParser {

  private def readOperation(
    sparkSession: SparkSession,
    source: SourceOrDestinationConfig,
    step: StepConfig
  ): DataFrame = {
    // This method reads data from a source based on the provided configuration and step.
    // It uses the S3DataReader to read data from S3.
    source.format match {
      case "S3" | "s3" =>
        println(s"Reading data from S3 source: ${source.alias} with properties: ${source.properties}")
        S3DataReader.readS3Data(sparkSession, source, step)
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported source format: ${source.format}. Only S3 is supported.")
    }
  }

  private def writeOperation(
                             sparkSession: SparkSession,
                             sink: SourceOrDestinationConfig,
                             step: StepConfig
                           ): Unit = {
    // This method reads data from a source based on the provided configuration and step.
    // It uses the S3DataReader to read data from S3.
    sink.format match {
      case "S3" | "s3" =>
        throw new UnsupportedOperationException(s"Unsupported source format: ${sink.format}. Only S3 is supported.")
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported source format: ${sink.format}. Only S3 is supported.")
    }
  }

  private def writeOperation(
                              sparkSession: SparkSession,
                              sink: SourceOrDestinationConfig,
                              step: StepConfig
                            ): Unit = {
    // This method reads data from a source based on the provided configuration and step.
    // It uses the S3DataReader to read data from S3.
    sink.format match {
      case "S3" | "s3" =>
        throw new UnsupportedOperationException(s"Unsupported source format: ${sink.format}. Only S3 is supported.")
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported source format: ${sink.format}. Only S3 is supported.")
    }
  }

  def operationHandler(
    sparkSession: SparkSession,
    step: StepConfig,
    sourcesMap: Map[String, SourceOrDestinationConfig],
    sinksMap: Map[String, SourceOrDestinationConfig]
  ): DataFrame = {
    // This method handles the operation specified in the step configuration.
    // It reads data from the source and writes it to the sink if applicable.
    step.operation match {
      case "read" =>
        readOperation(sparkSession, sourcesMap.getOrElse(step.source_alias.getOrElse(""), sinksMap(step.source_alias.getOrElse(""))), step)
      case "write" =>
        writeOperation(sparkSession, sourcesMap.getOrElse(step.source_alias.getOrElse(""), sinksMap(step.source_alias.getOrElse(""))), step)
      case "transform" =>
        writeOperation(sparkSession, sourcesMap.getOrElse(step.source_alias.getOrElse(""), sinksMap(step.source_alias.getOrElse(""))), step)
      case _ =>
        println(s"Unsupported operation: ${step.operation}")
        throw new UnsupportedOperationException(s"Unsupported operation: ${step.operation}. Only 'read' and 'write' operations are supported.")
    }
  }

  def runSteps(sparkSession: SparkSession, config: Config): Unit = {
    // This method can be used to run a series of steps in the application.
    // Currently, it is empty but can be expanded as needed.
    val steps = config.steps
    val sourcesMap = config.sources.map(s => s.alias -> s).toMap
    val sinksMap = config.sinks.map(s => s.alias -> s).toMap
    val dataFramesMap = new mutable.HashMap[String, DataFrame]()

    steps.foreach { step =>
      val df = operationHandler(sparkSession, step, sourcesMap, sinksMap)
      if (step.alias.isDefined) {
        dataFramesMap(step.alias.get) = df
        println(s"DataFrame for step '${step.alias.get}' stored in map.")
      } else {
        println(s"No alias defined for step, skipping storage of DataFrame.")
      }
    }
  }
}
