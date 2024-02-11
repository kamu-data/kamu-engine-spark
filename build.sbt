import sbtassembly.AssemblyPlugin.defaultUniversalScript

name := "kamu-engine-spark"
ThisBuild / organization := "dev.kamu"
ThisBuild / organizationName := "kamu.dev"
ThisBuild / startYear := Some(2018)
ThisBuild / licenses += ("Apache-2.0", new URL(
  "http://www.apache.org/licenses/LICENSE-2.0.html"
))
ThisBuild / scalaVersion := "2.12.18"

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
      deps.sparkCore % "provided",
      deps.sparkSql % "provided",
      deps.parquetColumn % "provided",
      deps.sedona % "provided",
      deps.sedonaGeotoolsWrapper % "provided",
      deps.sparkTestingBase % "test",
      deps.parquetAvro % "test",
      deps.avro % "test"
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
      deps.pureConfig,
      deps.pureConfigYaml
    ),
    commonSettings
  )

//////////////////////////////////////////////////////////////////////////////
// Dependencies
//////////////////////////////////////////////////////////////////////////////

lazy val versions = new {
  val sedona = "1.5.1"
  val geotoolsWrapper = "1.5.1-28.2"
  val pureConfig = "0.17.5"
  val spark = "3.5.0"
  val sparkTestingBase = s"${spark}_1.4.7"
  val parquet = "1.13.1" // From Spark pom.xml
}

lazy val deps =
  new {
    val slf4jApi = "org.slf4j" % "slf4j-api" % "1.7.30"
    // Configs
    val pureConfig = "com.github.pureconfig" %% "pureconfig" % versions.pureConfig
    val pureConfigYaml = "com.github.pureconfig" %% "pureconfig-yaml" % versions.pureConfig
    // Spark
    val sparkCore = "org.apache.spark" %% "spark-core" % versions.spark
    val sparkSql = "org.apache.spark" %% "spark-sql" % versions.spark
    // Sedona
    val sedona = "org.apache.sedona" %% "sedona-spark-shaded-3.5" % versions.sedona
    val sedonaGeotoolsWrapper = "org.datasyslab" % "geotools-wrapper" % versions.geotoolsWrapper
    // Test
    val scalaTest = "org.scalatest" %% "scalatest" % "3.2.18"
    val sparkHive = "org.apache.spark" %% "spark-hive" % versions.spark
    val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % versions.sparkTestingBase
    // Avro
    val parquetColumn = "org.apache.parquet" % "parquet-column" % versions.parquet
    val parquetAvro = "org.apache.parquet" % "parquet-avro" % versions.parquet
    val avro = "com.sksamuel.avro4s" %% "avro4s-core" % "3.1.1"
  }

//////////////////////////////////////////////////////////////////////////////
// Settings
//////////////////////////////////////////////////////////////////////////////

lazy val commonSettings = Seq()

lazy val sparkTestingSettings = Seq(
  Test / fork := true,
  Test / parallelExecution := false,
  // See: https://stackoverflow.com/questions/72724816/running-unit-tests-with-spark-3-3-0-on-java-17-fails-with-illegalaccesserror-cl
  javaOptions ++= Seq(
    "-Xms512M",
    "-Xmx2048M",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
  )
)
