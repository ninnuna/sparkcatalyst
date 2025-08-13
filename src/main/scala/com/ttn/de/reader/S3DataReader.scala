package com.ttn.de.reader

import com.ttn.de.config.parser.JsonConfigParser.{SourceOrDestinationConfig, StepConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

object S3DataReader {

  /**
   * Reads data from a specified S3 path using the provided SparkSession and configurations.
   * This function sets the S3 credentials on the SparkContext's Hadoop configuration before reading
   * and ensures they are cleared in a `finally` block.
   *
   * @param spark         The active SparkSession.
   * @param s3Path        The S3 path to read from. Can be a directory or a wildcard path.
   * @param fileFormat    The format of the data (e.g., "csv", "parquet", "json").
   * @param readConfigMap A map of configurations specific to the file format (e.g., "header" for CSV).
   * @param accessKey     Your AWS access key.
   * @param secretKey     Your AWS secret key.
   * @return A DataFrame containing the loaded data.
   */
  def readS3Data(
                  spark: SparkSession,
                  source: SourceOrDestinationConfig,
                  step: StepConfig
                ): DataFrame = {

    // Get the Hadoop configuration from the SparkContext
    val hadoopConfig = spark.sparkContext.hadoopConfiguration

    try {
      println(s"Setting S3 credentials for reading from S3 path:${step.properties("path")}")

      // Set the S3 credentials dynamically for the read operation
      hadoopConfig.set("fs.s3a.access.key", source.properties("aws_access_key_id"))
      hadoopConfig.set("fs.s3a.secret.key", source.properties("aws_secret_access_key"))
      hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      hadoopConfig.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

      println(s"Attempting to read data from S3 path: ${step.properties("path")} with format: ${step.properties("format")}")
      println(s"Applying read configurations: ${step.options.mkString(", ")}")

      val df = spark.read
        .format(step.properties("format"))
        .options(step.options.getOrElse(Map.empty[String, String]))
        .load(step.properties("path"))
      df
    } finally {
      println("Clearing S3 credentials from Hadoop configuration.")
      // Always unset the credentials in a finally block to ensure they are not
      // retained by the SparkSession for subsequent operations.
      hadoopConfig.set("fs.s3a.access.key", "")
      hadoopConfig.set("fs.s3a.secret.key", "")
    }
  }

}

