name := "transform-streaming"
version := "0.0.1"


// For testing with Spark
test in assembly := {}
fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")


libraryDependencies ++= Seq(
  // Config
  "com.github.pureconfig" %% "pureconfig" % "0.10.2",
  "com.github.pureconfig" %% "pureconfig-yaml" % "0.10.2",

  // Spark
  "org.apache.spark" %% "spark-core" % Versions.spark % "provided",
  "org.apache.spark" %% "spark-sql" % Versions.spark % "provided",

  // GeoSpark (should be installed in your spark)
  "org.datasyslab" % "geospark" % "1.1.3" % "provided",
  "org.datasyslab" % "geospark-sql_2.3" % "1.1.3" % "provided",

  // Testing
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.holdenkarau" %% "spark-testing-base" % s"${Versions.spark}_0.11.0" % "test",
  "org.apache.spark" %% "spark-hive" % Versions.spark % "test"
)
