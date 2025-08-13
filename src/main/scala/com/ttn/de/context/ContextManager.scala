package com.ttn.de.context

import com.ttn.de.config.parser.JsonConfigParser.ConfigFile
import org.apache.spark.sql.SparkSession

object ContextManager {

  // This object can be used to manage application context, such as Spark session,
  // configuration settings, or any other shared resources needed across the application.
  // Currently, it is empty but can be expanded as needed.

  // Example: You might want to add a Spark session here or any other shared context.
  def getSparkSession(config: ConfigFile): org.apache.spark.sql.SparkSession = {
    // Assuming you have Spark dependencies in your build file, you can create a Spark session like this:
    val spark: SparkSession = SparkSession.builder()
      .appName(config.config.app_properties("name"))
      .config(config.config.spark_properties)
      .master("local[*]") // Use local mode for testing; change as needed for production
      .getOrCreate()
    spark
  }
}