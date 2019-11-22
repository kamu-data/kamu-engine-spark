/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.transform.streaming

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

class Transform(config: AppConfig) {
  val logger = LogManager.getLogger(getClass.getName)
  val fileSystem = FileSystem.get(hadoopConf)

  def transform(): Unit = {
    logger.info("Starting transform.streaming")
    logger.info(s"Running with config: $config")

    val tasks = config.tasks.map(taskConfig => {
      val task = new TransformTask(
        fileSystem,
        config,
        getSparkSubSession(sparkSession),
        taskConfig
      )

      task.setupTransform()
      task
    })

    logger.info("Stream processing is running")

    // TODO: Using processAllAvailable() to block until we exhaust data
    // the use of this method is not recommended for prod
    tasks
      .flatMap(_.spark.streams.active)
      .foreach(_.processAllAvailable())

    logger.info("Finished")
    tasks.foreach(task => { task.spark.close() })
  }

  def sparkConf: SparkConf = {
    new SparkConf()
      .setAppName("transform.streaming")
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
