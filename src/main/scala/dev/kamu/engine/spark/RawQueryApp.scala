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

import dev.kamu.core.manifests.parsing.pureconfig.yaml
import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests._
import dev.kamu.core.utils.fs._
import org.apache.log4j.LogManager
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.sql.{AnalysisException, SparkSession}

import java.io.{PrintWriter, StringWriter}
import java.nio.file.Paths

object RawQueryApp {
  val requestPath = Paths.get("/opt/engine/in-out/request.yaml")
  val responsePath = Paths.get("/opt/engine/in-out/response.yaml")

  def main(args: Array[String]) {
    val logger = LogManager.getLogger(getClass.getName)

    if (!requestPath.toFile.exists())
      throw new RuntimeException(s"Could not find request config: $requestPath")

    val request = yaml.load[RawQueryRequest](requestPath)

    def saveResponse(response: RawQueryResponse): Unit = {
      yaml.save(response, responsePath)
    }

    logger.info(s"Executing raw query request: $request")

    val transform = new RawQuery(sparkSession)

    try {
      val response = transform.execute(request)
      saveResponse(response)
    } catch {
      case e: AnalysisException =>
        saveResponse(RawQueryResponse.InvalidQuery(e.toString))
        throw e
      case e: Exception =>
        val sw = new StringWriter()
        e.printStackTrace(new PrintWriter(sw))
        saveResponse(
          RawQueryResponse.InternalError(e.toString, Some(sw.toString))
        )
        throw e
    }

    logger.info(s"Done processing raw query")
  }

  def sparkSession: SparkSession = {
    val spark = SparkSession.builder
      .appName("kamu-transform")
      .getOrCreate()

    // TODO: For some reason registration of UDTs doesn't work from spark-defaults.conf
    SedonaSQLRegistrator.registerAll(spark)

    spark
  }
}
