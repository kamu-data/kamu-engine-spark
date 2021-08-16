/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest

import java.nio.file.Paths

import better.files.File
import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.Manifest
import dev.kamu.core.manifests.infra.IngestRequest
import dev.kamu.core.utils.ManualClock
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

object IngestApp {
  val requestPath = Paths.get("/opt/engine/in-out/request.yaml")
  val resultPath = Paths.get("/opt/engine/in-out/result.yaml")

  def main(args: Array[String]) {
    val logger = LogManager.getLogger(getClass.getName)

    if (!File(requestPath).exists)
      throw new RuntimeException(s"Could not find request config: $requestPath")

    val request = yaml.load[Manifest[IngestRequest]](requestPath).content

    val systemClock = new ManualClock()
    systemClock.advance()

    val ingest = new Ingest(systemClock)

    val result = ingest.ingest(getSparkSubSession(sparkSession), request)

    yaml.save(Manifest(result), resultPath)
  }

  def sparkConf: SparkConf = {
    new SparkConf()
      .setAppName("transform.streaming")
      .set("spark.sql.session.timeZone", "UTC")
  }

  def hadoopConf: Configuration = {
    new Configuration()
  }

  def sparkSession: SparkSession = {
    SparkSession.builder
      .config(sparkConf)
      .getOrCreate()
  }

  def getSparkSubSession(sparkSession: SparkSession): SparkSession = {
    sparkSession.newSession()
  }
}
