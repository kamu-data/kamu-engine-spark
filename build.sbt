name := "kamu-transform-streaming"
scalaVersion := "2.11.12"
version := "0.0.1"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  // Config
  "com.github.pureconfig" %% "pureconfig" % "0.10.1",

  // Spark
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // GeoSpark
  "org.datasyslab" % "geospark" % "1.1.3",
  "org.datasyslab" % "geospark-sql_2.3" % "1.1.3"
)
