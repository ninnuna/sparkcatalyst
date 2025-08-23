package com.ttn.de.transformer

import com.ttn.de.config.reader.JsonConfigParser.{SourceOrDestinationConfig, StepConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object TransformationHandler {

  def transformOperation(sparkSession: SparkSession,
                         df: DataFrame,
                         step: StepConfig,
                         dataFramesMap: mutable.HashMap[String, DataFrame]
                        ): DataFrame = {

    step.cmd match {
      case "sql" =>
        if (step.properties != null && step.properties.contains("query")) {
          println("Found SQL query in step configuration. Executing query...")

          // Find all matches in the string
          val matches: Iterator[String] = "cdf_[a-zA-Z0-9]+".r.findAllIn(step.properties.getOrElse("query", ""))

          // Print the results
          matches.foreach(name => {
            dataFramesMap.get(name) match {
              case Some(sdf) =>
                sdf.createOrReplaceTempView(name)
                println(s"Registered DataFrame with alias '$name' as a temporary view.")
              case None =>
                println(s"No DataFrame found with alias '$name'. Skipping registration.")
            }
          })

          // The executeSQL function will handle the actual query execution.
          executeSQL(sparkSession, step)
        } else {
          println("No SQL query found in step configuration. Returning original DataFrame.")
          // If no query is found, return the original DataFrame to continue the pipeline.
          df
        }
      case _ =>
        println(s"Unsupported cmd: ${step.cmd}")
        throw new UnsupportedOperationException(s"Unsupported operation: ${step.cmd}.")
    }
    // Check if the "query" key is present and its value is not null.

  }

  private def executeSQL(spark: SparkSession,
                         step: StepConfig
                        ): DataFrame = {
    // Retrieve the SQL query from the StepConfig properties map using the "query" key.
    val sqlQuery = step.properties.getOrElse("query",
      throw new IllegalArgumentException("The 'query' property is missing from the step configuration."))

    // Register the input DataFrame as a temporary view so it can be queried with SQL.
    println(s"Executing Spark SQL query:\n$sqlQuery")

    // Execute the SQL query and return the resulting DataFrame.
    spark.sql(sqlQuery)
  }
}
