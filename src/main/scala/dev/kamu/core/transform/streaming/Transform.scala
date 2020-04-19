/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.transform.streaming

import dev.kamu.core.manifests._
import dev.kamu.core.utils.Clock
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

class Transform(
  fileSystem: FileSystem,
  spark: SparkSession,
  systemClock: Clock
) {
  val logger = LogManager.getLogger(getClass.getName)

  def execute(
    datasetID: DatasetID,
    inputSlices: Map[DatasetID, InputSlice],
    transform: TransformKind.SparkSQL
  ): DataFrame = {
    // Setup inputs
    for ((inputID, slice) <- inputSlices)
      slice.dataFrame.createTempView(s"`$inputID`")

    // Setup transform
    for (step <- transform.queries) {
      spark
        .sql(step.query)
        .createTempView(s"`${step.alias.getOrElse(datasetID)}`")
    }

    // Process data
    spark.sql(s"SELECT * FROM `$datasetID`")
  }

}
