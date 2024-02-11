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
    if (request.inputDataPaths.nonEmpty) {
      spark.read
        .format("parquet")
        .parquet(request.inputDataPaths.map(_.toString): _*)
        .createTempView(inputViewName)
    }

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
      outputData
        .coalesce(1)
        .writeParquetSingleFile(request.outputDataPath)

    // Release memory
    outputData.unpersist(true)

    RawQueryResponse.Success(numRecords)
  }
}
