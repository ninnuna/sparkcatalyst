package com.ttn.de.config.reader

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

import java.io.File
import scala.io.Source

object LocalFsConfigReader {

  /**
   * Reads a file from the local file system and returns its content as a String.
   * This function is now focused solely on reading from a specific source.
   *
   * @param filePath The path to the JSON file.
   * @return An `Either` containing the file content as a String on success, or an error message.
   */
  def readJsonFromFileSystem(filePath: String): Either[String, String] = {
    var source: Option[Source] = None
    try {
      // Create a File object and a Source to read its contents.
      val file = new File(filePath)
      source = Some(Source.fromFile(file))

      // Read the entire file content into a single string.
      val jsonString = source.get.mkString
      Right(jsonString)
    } catch {
      case e: Exception =>
        Left(s"An error occurred while reading the file: ${e.getMessage}")
    } finally {
      // Ensure the file resource is closed, regardless of success or failure.
      source.foreach(_.close())
    }
  }
}
