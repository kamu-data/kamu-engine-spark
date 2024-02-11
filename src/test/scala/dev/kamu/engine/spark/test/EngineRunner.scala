/*
 * Copyright 2018 kamu.dev
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.kamu.engine.spark.test

import java.nio.file.{Path, Paths}
import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.utils.Temp
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.{DockerClient, DockerRunArgs}
import org.slf4j.LoggerFactory
import pureconfig.{ConfigReader, ConfigWriter}

import scala.reflect.ClassTag

class EngineRunner(
  dockerClient: DockerClient,
  image: String = "ghcr.io/kamu-data/engine-spark:0.23.0-spark_3.5.0"
) {
  private val logger = LoggerFactory.getLogger(getClass)

  def rawQuery(
    request: RawQueryRequest,
    workspaceDir: Path
  ): RawQueryResponse.Success = {
    submit[RawQueryRequest, RawQueryResponse](
      request,
      workspaceDir,
      "dev.kamu.engine.spark.RawQueryApp"
    ).asInstanceOf[RawQueryResponse.Success]
  }

  def executeTransform(
    request: TransformRequest,
    workspaceDir: Path
  ): TransformResponse.Success = {
    submit[TransformRequest, TransformResponse](
      request,
      workspaceDir,
      "dev.kamu.engine.spark.TransformApp"
    ).asInstanceOf[TransformResponse.Success]
  }

  def submit[Req: ClassTag, Resp: ClassTag](
    request: Req,
    workspaceDir: Path,
    appClass: String
  )(
    implicit dreq: ConfigWriter[Req],
    dresp: ConfigReader[Resp]
  ): Resp = {
    val engineJar = Paths.get("target", "scala-2.12", "engine.spark.jar")

    if (!engineJar.toFile.exists())
      throw new RuntimeException(s"Assembly does not exist: $engineJar")

    val inOutDirInContainer = Paths.get("/opt/engine/in-out")
    val engineJarInContainer = Paths.get("/opt/engine/bin/engine.spark.jar")

    Temp.withRandomTempDir("kamu-inout-") { inOutDir =>
      yaml.save(request, inOutDir.resolve("request.yaml"))

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

      yaml.load[Resp](inOutDir.resolve("response.yaml"))
    }
  }
}
