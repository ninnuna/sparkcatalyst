package com.ttn.de.writer

import com.ttn.de.config.reader.JsonConfigParser.{SourceOrDestinationConfig, StepConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

object WriteHandler {

  def writeOperation(
                      sparkSession: SparkSession,
                      df: DataFrame, // Add DataFrame as an argument
                      sink: SourceOrDestinationConfig,
                      step: StepConfig
                    ): DataFrame = {
    // This method writes data to a sink based on the provided configuration and step.
    // It uses the S3DataWriter to write the DataFrame to S3.
    sink.format.toLowerCase match {
      case "s3" =>
        S3DataWriter.writeS3Data(sparkSession, df, sink, step)
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported sink format: ${sink.format}. Only S3 is supported.")
    }
    sparkSession.emptyDataFrame
  }
}
