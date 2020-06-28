import sbtassembly.AssemblyPlugin.defaultUniversalScript

name := "kamu-engine-spark"
organization in ThisBuild := "dev.kamu"
organizationName in ThisBuild := "kamu.dev"
startYear in ThisBuild := Some(2018)
licenses in ThisBuild += ("MPL-2.0", new URL(
  "https://www.mozilla.org/en-US/MPL/2.0/"
))
scalaVersion in ThisBuild := "2.11.12"

//////////////////////////////////////////////////////////////////////////////
// Projects
//////////////////////////////////////////////////////////////////////////////

lazy val root = project
  .in(file("."))
  .aggregate(
    kamuCoreManifests,
    kamuCoreUtils
  )
  .dependsOn(
    kamuCoreUtils % "compile->compile;test->test",
    kamuCoreManifests
  )
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    libraryDependencies ++= Seq(
      deps.log4jApi,
      deps.betterFiles,
      deps.sparkCore % "provided",
      deps.sparkSql % "provided",
      deps.geoSpark % "provided",
      deps.geoSparkSql % "provided"
    ),
    commonSettings,
    sparkTestingSettings,
    aggregate in assembly := false,
    assemblySettings
  )

lazy val kamuCoreUtils = project
  .in(file("core.utils"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    libraryDependencies ++= Seq(
      deps.log4jApi,
      deps.betterFiles,
      deps.scalaTest % "test",
      deps.sparkCore % "provided",
      deps.sparkHive % "provided",
      deps.geoSpark % "test",
      deps.geoSparkSql % "test",
      deps.sparkTestingBase % "test",
      deps.sparkHive % "test"
    ),
    commonSettings,
    sparkTestingSettings
  )

lazy val kamuCoreManifests = project
  .in(file("core.manifests"))
  .dependsOn(
    kamuCoreUtils % "compile->compile;test->test"
  )
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    libraryDependencies ++= Seq(
      deps.betterFiles,
      deps.pureConfig,
      deps.pureConfigYaml,
      deps.spire
    ),
    commonSettings
  )

//////////////////////////////////////////////////////////////////////////////
// Dependencies
//////////////////////////////////////////////////////////////////////////////

lazy val versions = new {
  //val apacheCommonsCompress = "1.20"
  val betterFiles = "3.9.1"
  val geoSpark = "1.2.0"
  val log4j = "2.13.3"
  val pureConfig = "0.11.1"
  val spark = "2.4.0"
  val sparkTestingBase = s"${spark}_0.11.0"
  val spire = "0.13.0" // Used by spark too
}

lazy val deps =
  new {
    val log4jApi = "org.apache.logging.log4j" % "log4j-api" % versions.log4j
    // File System
    val betterFiles = "com.github.pathikrit" %% "better-files" % versions.betterFiles
    //val apacheCommonsCompress = "org.apache.commons" % "commons-compress" % versions.apacheCommonsCompress
    // Configs
    val pureConfig = "com.github.pureconfig" %% "pureconfig" % versions.pureConfig
    val pureConfigYaml = "com.github.pureconfig" %% "pureconfig-yaml" % versions.pureConfig
    // Spark
    val sparkCore = "org.apache.spark" %% "spark-core" % versions.spark
    val sparkSql = "org.apache.spark" %% "spark-sql" % versions.spark
    // GeoSpark
    val geoSpark = "org.datasyslab" % "geospark" % versions.geoSpark
    val geoSparkSql = "org.datasyslab" % "geospark-sql_2.3" % versions.geoSpark
    // Math
    // TODO: Using older version as it's also used by Spark
    //val spire = "org.typelevel" %% "spire" % versions.spire
    val spire = "org.spire-math" %% "spire" % versions.spire
    // Test
    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
    val sparkHive = "org.apache.spark" %% "spark-hive" % versions.spark
    val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % versions.sparkTestingBase
  }

//////////////////////////////////////////////////////////////////////////////
// Settings
//////////////////////////////////////////////////////////////////////////////

lazy val commonSettings = Seq()

lazy val sparkTestingSettings = Seq(
  fork in Test := true,
  parallelExecution in Test := false,
  javaOptions ++= Seq(
    "-Xms512M",
    "-Xmx2048M",
    "-XX:+CMSClassUnloadingEnabled"
  )
)

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := "engine.spark.jar",
  test in assembly := {}
)
