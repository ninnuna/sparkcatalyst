package com.ttn.de.config.parser

/**
 * This Scala application demonstrates how to read a JSON file from the local
 * file system and parse it into a case class structure that matches the
 * provided JSON format.
 *
 * It uses the Circe library for JSON parsing.
 *
 * NOTE: To run this code, you will need to add the following dependencies
 * to your project (e.g., in a `build.sbt` file for an SBT project):
 *
 * "io.circe" %% "circe-core" % "0.14.1"
 * "io.circe" %% "circe-generic" % "0.14.1"
 * "io.circe" %% "circe-parser" % "0.14.1"
 */

import com.ttn.de.config.reader.LocalFsConfigReader.readJsonFromFileSystem
import io.circe.generic.auto._
import io.circe.parser._

object JsonConfigParser {

  // Define the case classes that correspond to the structure of your JSON file.
  // Note the use of backticks for keywords like `type` to escape them.
  case class SourceOrDestinationConfig(alias: String, usage: String, format: String, properties: Map[String, String])

  case class StepConfig(
                         operation: String,
                         alias: Option[String], // Alias is an optional field
                         sink_alias: Option[String], // Sink alias is optional
                         source_alias: Option[String], // Source alias is optional
                         properties: Map[String, String],
                         options: Option[Map[String, String]] // Default to an empty map
                       )

  case class Config(
                     log_report_properties: Map[String, String],
                     app_properties: Map[String, String],
                     spark_properties: Map[String, String],
                     sources: List[SourceOrDestinationConfig],
                     sinks: List[SourceOrDestinationConfig],
                     steps: List[StepConfig]
                   )

  case class ConfigFile(config: Config)

  /**
   * Parses a JSON string into the `MyConfigurationFile` case class.
   * This function is now reusable for any JSON string, regardless of its source.
   *
   * @param jsonString The JSON content as a String.
   * @return An `Either` containing a `MyConfigurationFile` on success, or an error message.
   */
  private def parseConfiguration(jsonString: String): Either[String, ConfigFile] = {
    // Use Circe to parse the JSON string and decode it into our case class.
    decode[ConfigFile](jsonString) match {
      case Right(config) => Right(config)
      case Left(error) => Left(s"Error parsing JSON: ${error.getMessage}")
    }
  }

  private def getConfigFromSource(sourceType: String, sourcePath: String): Either[String, ConfigFile] = {
    println(s"Attempting to read and parse JSON file from: $sourcePath")

    val jsonContentEither = sourceType match {
      case "s3" =>
        // Placeholder for S3 reading logic, using Either.left for a controlled failure.
        // In a real implementation, you would call a function that reads from S3.
        Left("Reading from S3 is not implemented in this example.")
      case "local_fs" =>
        readJsonFromFileSystem(sourcePath)
      case _ =>
        Left(s"Unsupported source type: $sourceType")
    }

    // Chain the reading and parsing functions using flatMap for error handling.
    jsonContentEither.flatMap(parseConfiguration)
  }

  def getConfig(sourceType: String, sourcePath: String): Either[String, ConfigFile] = {
    getConfigFromSource(sourceType, sourcePath) match {
      case Right(config) =>
        println("Successfully read and parsed configuration:")
        println(s"Log Level: ${config.config.app_properties.getOrElse("log_level", "not specified")}")
        println(s"Number of sources: ${config.config.sources.length}")
        println(s"Number of destinations: ${config.config.sinks.length}")
        println(s"Number of steps: ${config.config.steps.length}")
        Right(config)
      case Left(error) =>
        println(s"Failed to read from local file system: $error")
        Left(error)
    }
  }
}
