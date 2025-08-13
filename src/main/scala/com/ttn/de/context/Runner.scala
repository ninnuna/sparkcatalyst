package com.ttn.de.context

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

import Binder.bindAppComponentsAndRun
import com.ttn.de.config.parser.JsonConfigParser

object Runner extends App {

  // --- Main execution block ---
  // Replace this with the actual path to your JSON file.
  private val jsonSourceType = args(0) // This can be 'local' or 's3' based on your requirement.
  private val jsonPath = args(1)

  println(s"Attempting to read and parse JSON file from: $jsonPath")

  // The main logic now chains the read and parse functions.
  val config = JsonConfigParser.getConfig(jsonSourceType, jsonPath).right.getOrElse {
    throw new RuntimeException("Failed to read and parse the JSON configuration file.")
  }

  bindAppComponentsAndRun(config)
}
