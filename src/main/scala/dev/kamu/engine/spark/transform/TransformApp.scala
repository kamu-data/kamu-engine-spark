/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.transform

import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.Manifest
import dev.kamu.core.manifests.infra.ExecuteQueryRequest
import dev.kamu.core.utils.ManualClock
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

object TransformApp {
  val requestPath = new Path("/opt/engine/in-out/request.yaml")
  val resultPath = new Path("/opt/engine/in-out/result.yaml")

  def main(args: Array[String]) {
    val logger = LogManager.getLogger(getClass.getName)

    val fileSystem = FileSystem.get(hadoopConf)

    if (!fileSystem.exists(requestPath))
      throw new RuntimeException(s"Could not find request config: $requestPath")

    val request =
      yaml.load[Manifest[ExecuteQueryRequest]](fileSystem, requestPath).content

    logger.info("Starting transform.streaming")
    logger.info(s"Executing request: $request")

    val systemClock = new ManualClock()
    systemClock.advance()

    logger.info(s"Processing dataset: ${request.datasetID}")

    val transform = new TransformExtended(
      fileSystem,
      getSparkSubSession(sparkSession),
      systemClock
    )

    val result = transform.executeExtended(request)

    yaml.save(Manifest(result), fileSystem, resultPath)
    logger.info(s"Done processing dataset: ${request.datasetID}")
  }

  def sparkConf: SparkConf = {
    new SparkConf()
      .setAppName("transform.streaming")
      .set("spark.sql.session.timeZone", "UTC")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
  }

  def hadoopConf: org.apache.hadoop.conf.Configuration = {
    SparkHadoopUtil.get.newConfiguration(sparkConf)
  }

  def sparkSession: SparkSession = {
    SparkSession.builder
      .config(sparkConf)
      .getOrCreate()
  }

  def getSparkSubSession(sparkSession: SparkSession): SparkSession = {
    val subSession = sparkSession.newSession()
    GeoSparkSQLRegistrator.registerAll(subSession)
    subSession
  }
}
