// This is the build.sbt file for the 'sparkcatalyst' project.
// It defines the project's name, Scala version, and its dependencies.

// Set the project's name.
name := "sparkcatalyst"

// Specify the Scala version to be used for compiling the project's code.
// This is set to 2.12.20 as requested.
scalaVersion := "2.12.20"

// Define the core project settings.
lazy val root = (project in file("."))
  .settings(
    // The main class for the application. This is optional but useful for running the project.
    // Replace 'com.example.Main' with the actual package and class of your main object.
    mainClass := Some("com.ttn.de"),

    // Add the library dependencies.
    // We are using '%%' to automatically append the Scala binary version (2.12 in this case)
    // to the artifact name.
    libraryDependencies ++= Seq(
      // The core Apache Spark dependency. It's often needed for basic Spark functionality.
      // We are using a 'provided' scope because this dependency will be provided by the
      // Spark cluster at runtime.
      "org.apache.spark" %% "spark-core" % "3.5.6",

      // The Spark SQL dependency, which includes the Catalyst optimizer.
      // This is also scoped as 'provided'.
      "org.apache.spark" %% "spark-sql" % "3.5.6",
      "io.circe" %% "circe-core" % "0.14.14",
      "io.circe" %% "circe-generic" % "0.14.14",
      "io.circe" %% "circe-parser" % "0.14.14"
    ),

    // --- Fix for IllegalAccessError on Java 9+ ---
    // The following two lines configure sbt to run the application in a forked JVM
    // and pass the necessary flag to allow access to internal Java APIs.
    // This is required to solve the "module java.base does not export sun.nio.ch" error.
    fork := true,
    javaOptions += "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
  )
