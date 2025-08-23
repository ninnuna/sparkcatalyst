package com.ttn.de.context

import com.ttn.de.config.reader.JsonConfigParser.{Config, ConfigFile, SourceOrDestinationConfig, StepConfig}
import com.ttn.de.config.transformer.StepConfigTransformer.identifyPersistenceLifetime
import com.ttn.de.reader.ReadHandler.readOperation
import com.ttn.de.transformer.TransformationHandler.transformOperation
import com.ttn.de.writer.WriteHandler.writeOperation
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object StepParser {

  private def operationHandler(
                                sparkSession: SparkSession,
                                df: DataFrame,
                                step: StepConfig,
                                sourcesMap: Map[String, SourceOrDestinationConfig],
                                sinksMap: Map[String, SourceOrDestinationConfig],
                                dataFramesMap: mutable.HashMap[String, DataFrame]
                              ): DataFrame = {
    // This method handles the operation specified in the step configuration.
    // It reads data from the source and writes it to the sink if applicable.
    step.operation match {
      case "read" =>
        readOperation(sparkSession, sourcesMap.getOrElse(step.source_alias.getOrElse(""), sinksMap(step.source_alias.getOrElse(""))), step)
      case "write" =>
        writeOperation(sparkSession, df, sourcesMap.getOrElse(step.source_alias.getOrElse(""), sinksMap(step.source_alias.getOrElse(""))), step)
      case "transform" =>
        transformOperation(sparkSession, df, step, dataFramesMap)
      case _ =>
        println(s"Unsupported operation: ${step.operation}")
        throw new UnsupportedOperationException(s"Unsupported operation: ${step.operation}. Only 'read' and 'write' operations are supported.")
    }
  }

  def runSteps(sparkSession: SparkSession, config: Config): Unit = {
    // This method can be used to run a series of steps in the application.
    // Currently, it is empty but can be expanded as needed.
    val steps = config.steps
    identifyPersistenceLifetime(steps)
    val sourcesMap = config.sources.map(s => s.alias -> s).toMap
    val sinksMap = config.sinks.map(s => s.alias -> s).toMap
    val dataFramesMap = new mutable.HashMap[String, DataFrame]()

    var df: DataFrame = sparkSession.emptyDataFrame
    steps.foreach { step =>
      df = operationHandler(sparkSession, df, step, sourcesMap, sinksMap, dataFramesMap)
      if (step.alias.isDefined) {
        if (config.app_properties.getOrElse("disk_usage", "AUTO").toUpperCase == "AGGRESSIVE") {
          dataFramesMap(step.alias.get) = df.persist(StorageLevel.DISK_ONLY)
          println(s"DataFrame for step '${step.alias.get}' persisted to disk.")
        }
        dataFramesMap(step.alias.get) = df
        println(s"DataFrame for step '${step.alias.get}' stored in map.")
        step.clearDfs.foreach(cdf => {
          dataFramesMap.get(cdf) match {
            case Some(sdf) =>
              if (sdf.storageLevel != StorageLevel.NONE) {
                sdf.unpersist()
              }
              dataFramesMap.remove(cdf)
              println(s"Cleared DataFrame with alias '$cdf' from memory and map.")
            case None =>
              println(s"No DataFrame found with alias '$cdf' to clear.")
          }
        })
      } else {
        println(s"No alias defined for step, skipping storage of DataFrame.")
      }
    }
  }
}
