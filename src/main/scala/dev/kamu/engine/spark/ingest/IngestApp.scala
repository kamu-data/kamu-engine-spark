/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest

import dev.kamu.core.manifests.infra.IngestConfig
import dev.kamu.core.utils.ManualClock
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

object IngestApp {
  def main(args: Array[String]) {
    val logger = LogManager.getLogger(getClass.getName)
    val config = IngestConfig.load()
    val systemClock = new ManualClock()
    val fileSystem = FileSystem.get(hadoopConf)

    // TODO: Disabling CRCs causes internal exception in Spark
    //fileSystem.setWriteChecksum(false)
    //fileSystem.setVerifyChecksum(false)

    if (config.tasks.isEmpty) {
      logger.warn("No tasks specified")
      return
    }

    val ingest = new Ingest(fileSystem, systemClock)

    for (task <- config.tasks) {
      systemClock.advance()
      ingest.ingest(getSparkSubSession(sparkSession), task)
    }
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
