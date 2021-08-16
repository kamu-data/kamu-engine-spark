/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark

import java.nio.file.{Path, Paths}
import better.files.File
import pureconfig.generic.auto._
import dev.kamu.core.manifests.Manifest
import dev.kamu.core.manifests.infra.{
  ExecuteQueryRequest,
  ExecuteQueryResult,
  IngestRequest,
  IngestResult
}
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.utils.Temp
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.{DockerClient, DockerRunArgs}
import org.slf4j.LoggerFactory
import pureconfig.{ConfigReader, ConfigWriter, Derivation}

class EngineRunner(
  dockerClient: DockerClient,
  image: String = "kamudata/engine-spark:0.11.0-spark_3.1.2"
) {
  private val logger = LoggerFactory.getLogger(getClass)

  def ingest(
    request: IngestRequest,
    workspaceDir: Path
  ): IngestResult = {
    submit[IngestRequest, IngestResult](
      request,
      workspaceDir,
      "dev.kamu.engine.spark.ingest.IngestApp"
    )
  }

  def submit[Req, Resp](
    request: Req,
    workspaceDir: Path,
    appClass: String
  )(
    implicit dreq: Derivation[ConfigWriter[Manifest[Req]]],
    dresp: Derivation[ConfigReader[Manifest[Resp]]]
  ): Resp = {
    val engineJar = Paths.get("target", "scala-2.12", "engine.spark.jar")

    if (!File(engineJar).exists)
      throw new RuntimeException(s"Assembly does not exist: $engineJar")

    val inOutDirInContainer = Paths.get("/opt/engine/in-out")
    val engineJarInContainer = Paths.get("/opt/engine/bin/engine.spark.jar")

    Temp.withRandomTempDir("kamu-inout-") { inOutDir =>
      yaml.save(Manifest(request), inOutDir.resolve("request.yaml"))

      try {

        dockerClient.runShell(
          DockerRunArgs(
            image = image,
            volumeMap = Map(
              engineJar -> engineJarInContainer,
              inOutDir -> inOutDirInContainer,
              workspaceDir -> workspaceDir
            )
          ),
          Array(
            "/opt/bitnami/spark/bin/spark-submit",
            "--master=local[4]",
            "--driver-memory=2g",
            s"--class=${appClass}",
            "/opt/engine/bin/engine.spark.jar"
          )
        )

      } finally {
        val unix = new com.sun.security.auth.module.UnixSystem()

        dockerClient.runShell(
          DockerRunArgs(
            image = image,
            volumeMap = Map(
              inOutDir -> inOutDirInContainer,
              workspaceDir -> workspaceDir
            )
          ),
          Array(
            "chown",
            "-R",
            s"${unix.getUid}:${unix.getGid}",
            workspaceDir.toString,
            inOutDirInContainer.toString
          )
        )

      }

      yaml
        .load[Manifest[Resp]](inOutDir.resolve("result.yaml"))
        .content
    }
  }
}
