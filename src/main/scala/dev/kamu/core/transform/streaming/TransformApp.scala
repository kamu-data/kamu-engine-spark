/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.transform.streaming

import dev.kamu.core.utils.ManualClock
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

object TransformApp {
  def main(args: Array[String]) {
    val logger = LogManager.getLogger(getClass.getName)
    val config = AppConfig.load()
    if (config.tasks.isEmpty) {
      logger.warn("No tasks specified")
      return
    }

    logger.info("Starting transform.streaming")
    logger.info(s"Running with config: $config")

    val fileSystem = FileSystem.get(hadoopConf)
    val systemClock = new ManualClock()

    for (taskConfig <- config.tasks) {
      systemClock.advance()
      logger.info(s"Processing dataset: ${taskConfig.datasetID}")

      val transform = new TransformExtended(
        fileSystem,
        getSparkSubSession(sparkSession),
        systemClock
      )

      transform.executeExtended(taskConfig)

      logger.info(s"Done processing dataset: ${taskConfig.datasetID}")
    }

    logger.info("Finished")
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
