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

package dev.kamu.engine.spark

import java.nio.file.Paths
import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.{TransformRequest, TransformResponse}
import org.apache.log4j.LogManager
import org.apache.sedona.spark.SedonaContext
import org.apache.spark.sql.{AnalysisException, SparkSession}

import java.io.{PrintWriter, StringWriter}

object TransformApp {
  val requestPath = Paths.get("/opt/engine/in-out/request.yaml")
  val responsePath = Paths.get("/opt/engine/in-out/response.yaml")

  def main(args: Array[String]) {
    val logger = LogManager.getLogger(getClass.getName)

    if (!requestPath.toFile.exists())
      throw new RuntimeException(s"Could not find request config: $requestPath")

    val request = yaml.load[TransformRequest](requestPath)
    def saveResponse(response: TransformResponse): Unit = {
      yaml.save(response, responsePath)
    }

    logger.info("Starting transform.streaming")
    logger.info(s"Executing request: $request")
    logger.info(
      s"Processing dataset: ${request.datasetAlias} (${request.datasetId})"
    )

    val transform = new Transform(sparkSession)

    try {
      val response = transform.execute(request)
      saveResponse(response)
    } catch {
      case e: AnalysisException =>
        saveResponse(TransformResponse.InvalidQuery(e.toString))
        throw e
      case e: Exception =>
        val sw = new StringWriter()
        e.printStackTrace(new PrintWriter(sw))
        saveResponse(
          TransformResponse.InternalError(e.toString, Some(sw.toString))
        )
        throw e
    }

    logger.info(
      s"Done processing dataset: ${request.datasetAlias} (${request.datasetId})"
    )
  }

  def sparkSession: SparkSession = {
    val spark = SparkSession.builder
      .appName("kamu-transform")
      .getOrCreate()

    // TODO: For some reason registration of UDTs doesn't work from spark-defaults.conf
    // See: https://sedona.apache.org/1.5.1/tutorial/sql/
    val _sedona = SedonaContext.create(spark)

    spark
  }
}
