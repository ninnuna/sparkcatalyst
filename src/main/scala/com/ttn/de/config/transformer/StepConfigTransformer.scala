package com.ttn.de.config.transformer

import com.ttn.de.config.reader.JsonConfigParser.StepConfig
import org.apache.spark.sql.DataFrame

object StepConfigTransformer {

  def identifyPersistenceLifetime(
                                   steps: List[StepConfig],
                                 ): Unit = {
    val cdfSet = scala.collection.mutable.Set[String]()
    steps.reverse.foreach(step => step.operation match {
      case "transform" =>
        step.cmd.get match {
          case "sql" =>
            if (step.properties != null && step.properties.contains("query")) {
              println("Found SQL query in step configuration. Executing query...")
              // Find all matches in the string
              val matches: List[String] = "cdf_[a-zA-Z0-9]+".r.findAllIn(step.properties.getOrElse("query", "")).toList
              step.clearDfs = Option(matches.diff(cdfSet.toList).distinct)
              cdfSet ++= matches
              println(s"DataFrames to clear for step '${step.alias.getOrElse("unknown")}': ${step.clearDfs.mkString(", ")}")
            }
        }
      case "write" =>
        if (step.properties != null && step.properties.contains("by_alias")) {
          val byAlias = step.properties("by_alias")
          if (!cdfSet.contains(byAlias)) {
            step.clearDfs = Option(List(byAlias))
            cdfSet += byAlias
            println(s"DataFrame to clear for write step '${step.alias.getOrElse("unknown")}': $byAlias")
          }
        }
      case _ =>
        println(s"Skipping operation: ${step.operation}")
    })
    // Check if the "query" key is present and its value is not null.
  }
}
