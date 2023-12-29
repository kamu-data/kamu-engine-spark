/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.transform

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
import dev.kamu.core.manifests.{
  ExecuteQueryRequest,
  ExecuteQueryResponse,
  ExecuteQueryInput
}
import dev.kamu.core.utils.fs._
import dev.kamu.engine.spark.ingest.utils.DFUtils._
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
    request: ExecuteQueryRequest
  ): ExecuteQueryResponse = {
    val transform = loadTransform(request.transform)

    val vocab = request.vocab.withDefaults()

    val inputWatermarks =
      getInputWatermarks(request.queryInputs, request.prevCheckpointPath)

    // Setup inputs
    val dataFrames = request.queryInputs
      .map(input => {
        val df = prepareInputDataframe(spark, input)
        df.cache()
        df.createTempView(input.queryAlias)
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

    if (result.getColumn(vocab.offsetColumn.get).isDefined)
      throw new Exception(
        s"Transformed data contains a column that conflicts with the system column name, " +
          s"you should either rename the data column or configure the dataset vocabulary " +
          s"to use a different name: ${vocab.offsetColumn.get}"
      )

    if (result.getColumn(vocab.systemTimeColumn.get).isDefined)
      throw new Exception(
        s"Transformed data contains a column that conflicts with the system column name, " +
          s"you should either rename the data column or configure the dataset vocabulary " +
          s"to use a different name: ${vocab.systemTimeColumn.get}"
      )

    if (result.getColumn(vocab.eventTimeColumn.get).isEmpty)
      throw new Exception(
        s"Transformed data does not contains an event time column: ${vocab.eventTimeColumn.get}"
      )

    val eventTimeType =
      result.schema(vocab.eventTimeColumn.get).dataType.typeName
    if (!Array("timestamp", "date").contains(eventTimeType))
      throw new RuntimeException(
        s"Event time column can only be TIMESTAMP or DATE, got: $eventTimeType"
      )

    val window =
      Window.partitionBy(lit(0)).orderBy(lit(vocab.eventTimeColumn.get))

    result = result
      .coalesce(1)
      .orderBy(vocab.eventTimeColumn.get)
      .withColumn(
        vocab.offsetColumn.get,
        row_number().over(window) + (request.nextOffset - 1)
      )
      .withColumn(
        vocab.systemTimeColumn.get,
        lit(Timestamp.from(request.systemTime))
      )
      .columnToFront(
        vocab.offsetColumn.get,
        vocab.systemTimeColumn.get
      )

    result.cache()

    // Write input watermarks in case they will not be passed during next run
    for ((datasetId, watermark) <- inputWatermarks) {
      writeWatermark(datasetId, request.newCheckpointPath, watermark)
    }

    // Write data
    if (!result.isEmpty)
      writeParquet(result, request.newDataPath)

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

    ExecuteQueryResponse.Success(
      newOffsetInterval = newOffsetInterval,
      newWatermark = newWatermark
    )
  }

  private def prepareInputDataframe(
    spark: SparkSession,
    input: ExecuteQueryRequestInput
  ): DataFrame = {
    val vocab = input.vocab.withDefaults()

    // TODO: use schema from metadata
    val df = spark.read
      .format("parquet")
      .parquet(input.dataPaths.map(_.toString): _*)

    input.offsetInterval match {
      case None =>
        df.where(lit(false))
      case Some(iv) =>
        val col = df.col(vocab.offsetColumn.get)
        df.filter(col >= iv.start && col <= iv.end)
    }
  }

  private def writeParquet(df: DataFrame, outPath: Path): Unit = {
    val outDir = outPath.getParent
    val tmpOutDir = outDir.resolve(".tmp")

    df.write.parquet(tmpOutDir.toString)

    val dataFiles = File(tmpOutDir).glob("*.snappy.parquet").toList

    if (dataFiles.length != 1)
      throw new RuntimeException(
        "Unexpected number of files in output directory:\n" + File(tmpOutDir).list
          .map(_.path.toString)
          .mkString("\n")
      )

    val dataFile = dataFiles.head.path

    File(dataFile).moveTo(outPath)
    File(tmpOutDir).delete()
  }

  private def getInputWatermarks(
    inputs: Vector[ExecuteQueryRequestInput],
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

  private def loadTransform(
    raw: dev.kamu.core.manifests.Transform
  ): Transform.Sql = {
    if (raw.engine != "spark")
      throw new RuntimeException(s"Unsupported engine: ${raw.engine}")

    val sql = raw.asInstanceOf[Transform.Sql]

    sql.copy(
      queries =
        if (sql.query.isDefined) Some(Vector(SqlQueryStep(None, sql.query.get)))
        else sql.queries
    )
  }

  private def maxOption[T: Ordering](seq: Seq[T]): Option[T] = {
    if (seq.isEmpty)
      None
    else
      Some(seq.max)
  }

}
