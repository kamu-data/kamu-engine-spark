/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark

import java.io.PrintWriter
import java.nio.file.Path
import java.sql.Timestamp
import java.time.Instant
import java.util.Scanner
import better.files.File
import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests._
import dev.kamu.core.utils.fs._
import DFUtils._
import org.apache.log4j.LogManager
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Some logic eventually should be moved to the coordinator side **/
class Transform(
  spark: SparkSession
) {
  private val outputViewName = "__output__"
  private val logger = LogManager.getLogger(getClass.getName)

  def execute(
    request: TransformRequest
  ): TransformResponse = {
    val transform = request.transform.asInstanceOf[Transform.Sql]
    assert(transform.engine.toLowerCase == "spark")

    val inputWatermarks =
      getInputWatermarks(request.queryInputs, request.prevCheckpointPath)

    // Setup inputs
    val dataFrames = request.queryInputs
      .map(input => {
        val df = prepareInputDataframe(spark, input)
        df.cache()
        df.createTempView(s"`${input.queryAlias}`")
        df
      })

    // Setup transform
    for (step <- transform.queries.get) {
      val name = step.alias.getOrElse(outputViewName)
      spark
        .sql(step.query)
        .createTempView(s"`$name`")
    }

    // Process data
    var result = spark
      .sql(s"SELECT * FROM `$outputViewName`")

    val vocab = request.vocab

    if (result.getColumn(vocab.offsetColumn).isDefined)
      throw new Exception(
        s"Transformed data contains a column that conflicts with the system column name, " +
          s"you should either rename the data column or configure the dataset vocabulary " +
          s"to use a different name: ${vocab.offsetColumn}"
      )

    if (result.getColumn(vocab.systemTimeColumn).isDefined)
      throw new Exception(
        s"Transformed data contains a column that conflicts with the system column name, " +
          s"you should either rename the data column or configure the dataset vocabulary " +
          s"to use a different name: ${vocab.systemTimeColumn}"
      )

    if (result.getColumn(vocab.eventTimeColumn).isEmpty)
      throw new Exception(
        s"Transformed data does not contains an event time column: ${vocab.eventTimeColumn}"
      )

    val eventTimeType =
      result.schema(vocab.eventTimeColumn).dataType.typeName
    if (!Array("timestamp", "date").contains(eventTimeType))
      throw new RuntimeException(
        s"Event time column can only be TIMESTAMP or DATE, got: $eventTimeType"
      )

    val window =
      Window.partitionBy(lit(0)).orderBy(lit(vocab.eventTimeColumn))

    result = result
      .coalesce(1)
      .orderBy(vocab.eventTimeColumn)
      .withColumn(
        vocab.offsetColumn,
        row_number().over(window) + (request.nextOffset - 1)
      )
      .withColumn(
        vocab.systemTimeColumn,
        lit(Timestamp.from(request.systemTime))
      )
      .columnToFront(
        vocab.offsetColumn,
        vocab.systemTimeColumn
      )

    result.cache()

    // Write input watermarks in case they will not be passed during next run
    for ((datasetId, watermark) <- inputWatermarks) {
      writeWatermark(datasetId, request.newCheckpointPath, watermark)
    }

    // Write data
    if (!result.isEmpty)
      result.writeParquetSingleFile(request.newDataPath)

    val newOffsetInterval =
      if (!result.isEmpty)
        Some(
          OffsetInterval(
            start = request.nextOffset,
            end = request.nextOffset + result.count() - 1
          )
        )
      else None

    val newWatermark = Some(inputWatermarks.values.min)

    // Release memory
    result.unpersist(true)
    dataFrames.foreach(_.unpersist(true))

    TransformResponse.Success(
      newOffsetInterval = newOffsetInterval,
      newWatermark = newWatermark
    )
  }

  private def prepareInputDataframe(
    spark: SparkSession,
    input: TransformRequestInput
  ): DataFrame = {
    // TODO: use schema from metadata
    val df = spark.read
      .format("parquet")
      .parquet(input.dataPaths.map(_.toString): _*)

    input.offsetInterval match {
      case None =>
        df.where(lit(false))
      case Some(iv) =>
        val col = df.col(input.vocab.offsetColumn)
        df.filter(col >= iv.start && col <= iv.end)
    }
  }

  private def getInputWatermarks(
    inputs: Vector[TransformRequestInput],
    prevCheckpointDir: Option[Path]
  ): Map[DatasetId, Instant] = {
    val previousWatermarks: Map[DatasetId, Instant] =
      if (prevCheckpointDir.isDefined) {
        inputs
          .map(
            input =>
              (
                input.datasetId,
                readWatermark(input.datasetId, prevCheckpointDir.get)
              )
          )
          .toMap
      } else {
        Map.empty
      }

    inputs
      .map(input => {
        (
          input.datasetId,
          maxOption(input.explicitWatermarks.map(_.eventTime))
            .getOrElse(previousWatermarks(input.datasetId))
        )
      })
      .toMap
  }

  private def readWatermark(
    datasetId: DatasetId,
    checkpointDir: Path
  ): Instant = {
    val wmPath = checkpointDir.resolve(s"${datasetId.toMultibase()}.watermark")
    val reader = new Scanner(File(wmPath).newInputStream)
    val watermark = Instant.parse(reader.nextLine())
    reader.close()
    watermark
  }

  private def writeWatermark(
    datasetId: DatasetId,
    checkpointDir: Path,
    watermark: Instant
  ): Unit = {
    File(checkpointDir).createDirectories()
    val outputStream = File(
      checkpointDir.resolve(s"${datasetId.toMultibase()}.watermark")
    ).newOutputStream
    val writer = new PrintWriter(outputStream)
    writer.println(watermark.toString)
    writer.close()
    outputStream.close()
  }

  private def maxOption[T: Ordering](seq: Seq[T]): Option[T] = {
    if (seq.isEmpty)
      None
    else
      Some(seq.max)
  }

}
