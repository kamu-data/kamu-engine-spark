lazy val kamuCoreManifests = RootProject(file("../kamu-core-manifests"))

lazy val kamuCoreTransformStreaming = (project in file("."))
  .aggregate(kamuCoreManifests)
  .dependsOn(kamuCoreManifests)
  .settings(
    scalaVersion := "2.11.12",
    organization := "dev.kamu",
    organizationName := "kamu",
    name := "kamu-core-transform-streaming",
    version := "0.1.0-SNAPSHOT",
    libraryDependencies ++= Seq(
      // Internal
      "dev.kamu" %% "kamu-core-manifests" % "0.1.0-SNAPSHOT",
      // Config
      "com.github.pureconfig" %% "pureconfig" % "0.10.2",
      "com.github.pureconfig" %% "pureconfig-yaml" % "0.10.2",
      // Spark
      "org.apache.spark" %% "spark-core" % Versions.spark % "provided",
      "org.apache.spark" %% "spark-sql" % Versions.spark % "provided",
      // GeoSpark
      "org.datasyslab" % "geospark" % "1.2.0",
      "org.datasyslab" % "geospark-sql_2.3" % "1.2.0",
      // Testing
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "com.holdenkarau" %% "spark-testing-base" % s"${Versions.spark}_0.11.0" % "test",
      "org.apache.spark" %% "spark-hive" % Versions.spark % "test"
    ),
    // For testing with Spark
    test in assembly := {},
    fork in Test := true,
    parallelExecution in Test := false,
    javaOptions ++= Seq(
      "-Xms512M",
      "-Xmx2048M",
      "-XX:+CMSClassUnloadingEnabled"
    )
  )
