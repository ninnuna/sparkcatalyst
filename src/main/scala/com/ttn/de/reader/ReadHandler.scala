package com.ttn.de.reader

import com.ttn.de.config.reader.JsonConfigParser.{SourceOrDestinationConfig, StepConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadHandler {

  def readOperation(
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
}
