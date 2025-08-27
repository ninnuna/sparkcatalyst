package com.ttn.de.writer

import com.ttn.de.config.reader.JsonConfigParser.{SourceOrDestinationConfig, StepConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

object S3DataWriter {

  /**
   * Writes a DataFrame to a specified S3 path using the provided SparkSession and configurations.
   * This function dynamically sets the S3 credentials on the SparkContext's Hadoop configuration
   * for the write operation and ensures they are cleared in a `finally` block.
   *
   * @param spark  The active SparkSession.
   * @param df     The DataFrame to be written to S3.
   * @param source The source/destination configuration containing S3 credentials.
   * @param step   The step configuration containing the S3 path, file format, and write options.
   */
  def writeS3Data(
                   spark: SparkSession,
                   df: DataFrame,
                   source: SourceOrDestinationConfig,
                   step: StepConfig
                 ): Unit = {

    // Get the Hadoop configuration from the SparkContext
    val hadoopConfig = spark.sparkContext.hadoopConfiguration

    try {
      println(s"Setting S3 credentials for writing to S3 path: ${step.properties("path")}")

      // Set the S3 credentials dynamically for the write operation
      hadoopConfig.set("fs.s3a.access.key", source.properties("aws_access_key_id"))
      hadoopConfig.set("fs.s3a.secret.key", source.properties("aws_secret_access_key"))
      hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      hadoopConfig.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

      println(s"Attempting to write data to S3 path: ${step.properties("path")} with format: ${step.properties("format")}")
      println(s"Applying write configurations: ${step.options.mkString(", ")}")

      // Write the DataFrame to S3 using the specified format and options
      println(step.options)
      var dfw = df.repartition(4).write
        .format(step.properties("format"))
        .options(step.options.getOrElse(Map.empty[String, String]))
      if (step.properties.getOrElse("""partition_columns""", "").nonEmpty) {
        dfw = dfw.partitionBy(step.properties("partition_columns").split(","): _*)
      }
      dfw.mode(step.properties.getOrElse("mode", "overwrite")).save(step.properties("path"))

      println(s"Data successfully written to S3 path: ${step.properties("path")}")
    } finally {
      println("Clearing S3 credentials from Hadoop configuration.")
      // Always unset the credentials in a finally block to ensure they are not
      // retained by the SparkSession for subsequent operations.
      hadoopConfig.set("fs.s3a.access.key", "")
      hadoopConfig.set("fs.s3a.secret.key", "")
    }
  }

}

