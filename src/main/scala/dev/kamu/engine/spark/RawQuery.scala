/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark

import dev.kamu.core.manifests._
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import DFUtils._

class RawQuery(
  spark: SparkSession
) {
  private val inputViewName = "input"
  private val outputViewName = "__output__"
  private val logger = LogManager.getLogger(getClass.getName)

  def execute(
    request: RawQueryRequest
  ): RawQueryResponse = {
    val transform = request.transform.asInstanceOf[Transform.Sql]
    assert(transform.engine.toLowerCase == "spark")

    // Setup input
    spark.read
      .format("parquet")
      .parquet(request.inputDataPaths.map(_.toString): _*)
      .createTempView(inputViewName)

    // Setup transform
    for (step <- transform.queries.get) {
      val name = step.alias.getOrElse(outputViewName)
      spark
        .sql(step.query)
        .createTempView(s"`$name`")
    }

    // Process data
    val outputData = spark
      .sql(s"SELECT * FROM `$outputViewName`")

    // TODO: PERF: Consider writing to file first and then counting records
    outputData.cache()
    val numRecords = outputData.count()

    // Write data
    if (numRecords != 0)
      outputData.writeParquetSingleFile(request.outputDataPath)

    // Release memory
    outputData.unpersist(true)

    RawQueryResponse.Success(numRecords)
  }
}
