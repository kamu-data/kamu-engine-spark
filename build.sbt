import sbtassembly.AssemblyPlugin.defaultUniversalScript

name := "kamu-engine-spark"
ThisBuild / organization := "dev.kamu"
ThisBuild / organizationName := "kamu.dev"
ThisBuild / startYear := Some(2018)
ThisBuild / licenses += ("MPL-2.0", new URL(
  "https://www.mozilla.org/en-US/MPL/2.0/"
))
ThisBuild / scalaVersion := "2.12.12"

// Needed by GeoSpark SNAPSHOT version
ThisBuild / resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

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
      deps.slf4jApi,
      deps.betterFiles,
      deps.sparkCore % "provided",
      deps.sparkSql % "provided",
      deps.sedona % "provided",
      deps.sedonaGeotoolsWrapper % "provided",
      deps.sparkTestingBase % "test",
      deps.parquet % "test",
      deps.avro % "test"
    ),
    dependencyOverrides ++= Seq(
      // TODO: No f'ing idea why 2.12.2 version is being used why dependencyTree and other tools show only 2.10.0 being used
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0" % "test"
    ),
    commonSettings,
    sparkTestingSettings,
    assembly / aggregate := false,
    assembly / assemblyJarName := "engine.spark.jar",
    assembly / test := {}
  )

lazy val kamuCoreUtils = project
  .in(file("core.utils"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    libraryDependencies ++= Seq(
      deps.slf4jApi,
      deps.betterFiles,
      deps.scalaTest % "test"
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
      deps.pureConfigYaml
    ),
    commonSettings
  )

//////////////////////////////////////////////////////////////////////////////
// Dependencies
//////////////////////////////////////////////////////////////////////////////

lazy val versions = new {
  val betterFiles = "3.9.1"
  val sedona = "1.0.1-incubating"
  val pureConfig = "0.13.0"
  val spark = "3.1.2"
  val sparkTestingBase = s"${spark}_1.1.0"
}

lazy val deps =
  new {
    val slf4jApi = "org.slf4j" % "slf4j-api" % "1.7.30"
    // File System
    val betterFiles = "com.github.pathikrit" %% "better-files" % versions.betterFiles
    //val apacheCommonsCompress = "org.apache.commons" % "commons-compress" % versions.apacheCommonsCompress
    // Configs
    val pureConfig = "com.github.pureconfig" %% "pureconfig" % versions.pureConfig
    val pureConfigYaml = "com.github.pureconfig" %% "pureconfig-yaml" % versions.pureConfig
    // Spark
    val sparkCore = "org.apache.spark" %% "spark-core" % versions.spark
    val sparkSql = "org.apache.spark" %% "spark-sql" % versions.spark
    // Sedona
    val sedona = "org.apache.sedona" %% "sedona-python-adapter-3.0" % versions.sedona
    val sedonaGeotoolsWrapper = "org.datasyslab" % "geotools-wrapper" % "geotools-24.1"
    // Test
    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
    val sparkHive = "org.apache.spark" %% "spark-hive" % versions.spark
    val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % versions.sparkTestingBase
    // Avro
    val parquet = "org.apache.parquet" % "parquet-avro" % "1.11.1"
    var avro = "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.4"
  }

//////////////////////////////////////////////////////////////////////////////
// Settings
//////////////////////////////////////////////////////////////////////////////

lazy val commonSettings = Seq()

lazy val sparkTestingSettings = Seq(
  Test / fork := true,
  Test / parallelExecution := false,
  javaOptions ++= Seq(
    "-Xms512M",
    "-Xmx2048M",
    "-XX:+CMSClassUnloadingEnabled"
  )
)
