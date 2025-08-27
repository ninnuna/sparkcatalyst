package com.ttn.de.context

import com.ttn.de.config.reader.JsonConfigParser.ConfigFile
import com.ttn.de.context.StepParser.runSteps

object Binder {

  def bindAppComponentsAndRun(config: ConfigFile): Unit = {
    // This method can be used to bind application components and run the application.
    // Currently, it is empty but can be expanded as needed.
    // Example: You might want to initialize a Spark session or load configurations here.
    val sparkSession = ContextManager.getSparkSession(config)
    try {
      println(sparkSession.sparkContext.appName)
      runSteps(sparkSession, config.config)
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      // 6. Stop the SparkSession
      while (true){
        Thread.sleep(10000)
      }
      sparkSession.stop()
      println("SparkSession stopped.")
    }
  }

}
