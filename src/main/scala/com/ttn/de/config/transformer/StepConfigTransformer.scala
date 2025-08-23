package com.ttn.de.config.transformer

import com.ttn.de.config.reader.JsonConfigParser.StepConfig
import org.apache.spark.sql.DataFrame

object StepConfigTransformer {

  def identifyPersistenceLifetime(
                                   steps: List[StepConfig],
                                 ): DataFrame = {
    val cdfSet = scala.collection.mutable.Set[String]()
    steps.reverse.foreach(step => step.operation match {
      case "transform" =>
        step.cmd match {
          case "sql" =>
            if (step.properties != null && step.properties.contains("query")) {
              println("Found SQL query in step configuration. Executing query...")

              // Find all matches in the string
              val matches: Iterator[String] = "cdf_[a-zA-Z0-9]+".r.findAllIn(step.properties.getOrElse("query", ""))
              step.clearDfs = matches.toList.diff(cdfSet.toList).distinct
              cdfSet ++= matches.toList
              println(s"DataFrames to clear for step '${step.alias.getOrElse("unknown")}': ${step.clearDfs.mkString(", ")}")
            }
        }
      case "write" =>
        println(s"Unsupported cmd: ${step.cmd}")
        throw new UnsupportedOperationException(s"Unsupported operation: ${step.cmd}.")
      case _ =>
        println(s"Unsupported cmd: ${step.cmd}")
        throw new UnsupportedOperationException(s"Unsupported operation: ${step.cmd}.")
    })
    // Check if the "query" key is present and its value is not null.
  }
}
