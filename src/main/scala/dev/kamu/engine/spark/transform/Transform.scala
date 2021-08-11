/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.transform

import java.io.PrintWriter
import java.nio.file.{Path, Paths}
import java.sql.Timestamp
import java.time.Instant
import java.util.Scanner

import better.files.File
import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.infra.{
  ExecuteQueryRequest,
  ExecuteQueryResult,
  InputDataSlice
}
import dev.kamu.core.utils.{Clock, DataFrameDigestSHA256}
import dev.kamu.core.utils.fs._
import dev.kamu.engine.spark.ingest.utils.DFUtils._
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import spire.math.{Empty, Interval}
import spire.math.interval.{Closed, Open, Unbound}

case class InputSlice(dataFrame: DataFrame, interval: Interval[Instant])

/** Some logic eventually should be moved to the coordinator side **/
class Transform(
  spark: SparkSession,
  systemClock: Clock
) {
  private val logger = LogManager.getLogger(getClass.getName)
  private val zero_hash =
    "0000000000000000000000000000000000000000000000000000000000000000"

  def execute(request: ExecuteQueryRequest): ExecuteQueryResult = {
    val transform = loadTransform(request.source.transform)

    val prevCheckpointDir = request.prevCheckpointDir.map(Paths.get(_))
    val newCheckpointDir = Paths.get(request.newCheckpointDir)
    File(newCheckpointDir).createDirectories()

    val resultVocab =
      request.datasetVocabs(request.datasetID.toString).withDefaults()

    val inputSlices =
      prepareInputSlices(
        spark,
        typedMap(request.inputSlices),
        typedMap(request.datasetVocabs)
      )

    val inputWatermarks =
      getInputWatermarks(typedMap(request.inputSlices), prevCheckpointDir)

    inputSlices.values.foreach(_.dataFrame.cache())

    // Setup inputs
    for ((inputID, slice) <- inputSlices)
      slice.dataFrame.createTempView(s"`$inputID`")

    // Setup transform
    for (step <- transform.queries.get) {
      spark
        .sql(step.query)
        .createTempView(s"`${step.alias.getOrElse(request.datasetID)}`")
    }

    // Process data
    val result = spark
      .sql(s"SELECT * FROM `${request.datasetID}`")
      .orderBy(resultVocab.eventTimeColumn.get)
      .coalesce(1)

    result.cache()

    // Prepare metadata
    val block = MetadataBlock(
      blockHash = zero_hash,
      prevBlockHash = None,
      systemTime = systemClock.instant(),
      outputSlice =
        if (!result.isEmpty)
          Some(
            DataSlice(
              hash = computeHash(result),
              interval =
                if (result.isEmpty) Interval.empty
                else Interval.point(systemClock.instant()),
              numRecords = result.count()
            )
          )
        else None,
      // Output's watermark is a minimum of input watermarks
      outputWatermark = Some(inputWatermarks.values.min),
      inputSlices = Some(request.source.inputs.map(id => {
        val slice = inputSlices(id)
        DataSlice(
          hash = computeHash(slice.dataFrame),
          interval = slice.interval,
          numRecords = slice.dataFrame.count()
        )
      }))
    )

    if (result.getColumn(resultVocab.systemTimeColumn.get).isDefined)
      throw new Exception(
        s"Transformed data contains a column that conflicts with the system column name, " +
          s"you should either rename the data column or configure the dataset vocabulary " +
          s"to use a different name: ${resultVocab.systemTimeColumn.get}"
      )

    val resultWithSystemTime = result
      .withColumn(
        resultVocab.systemTimeColumn.get,
        lit(systemClock.timestamp())
      )
      .columnToFront(resultVocab.systemTimeColumn.get)

    // Write input watermarks in case they will not be passed during next run
    for ((datasetID, watermark) <- inputWatermarks) {
      writeWatermark(datasetID, newCheckpointDir, watermark)
    }

    // Write data
    if (!result.isEmpty)
      writeParquet(resultWithSystemTime, Paths.get(request.outDataPath))

    // Release memory
    result.unpersist(true)
    inputSlices.values.foreach(_.dataFrame.unpersist(true))

    ExecuteQueryResult(block = block)
  }

  private def prepareInputSlices(
    spark: SparkSession,
    inputSlices: Map[DatasetID, InputDataSlice],
    inputVocabs: Map[DatasetID, DatasetVocabulary]
  ): Map[DatasetID, InputSlice] = {
    inputSlices.map({
      case (id, slice) =>
        val inputSlice =
          prepareInputSlice(
            spark,
            id,
            slice,
            inputVocabs(id).withDefaults()
          )
        (id, inputSlice)
    })
  }

  private def prepareInputSlice(
    spark: SparkSession,
    id: DatasetID,
    slice: InputDataSlice,
    vocab: DatasetVocabulary
  ): InputSlice = {
    // TODO: use schema from metadata
    // TODO: use individually provided files instead of always reading all files
    val dataDir = Paths.get(slice.schemaFile).getParent

    val df = spark.read
      .parquet(dataDir.toString)
      .transform(sliceData(slice.interval, vocab))
      .drop(vocab.systemTimeColumn.get)

    InputSlice(
      dataFrame = df,
      interval = slice.interval
    )
  }

  private def sliceData(interval: Interval[Instant], vocab: DatasetVocabulary)(
    df: DataFrame
  ): DataFrame = {
    interval match {
      case Empty() =>
        df.where(lit(false))
      case _ =>
        val col = df.col(vocab.systemTimeColumn.get)

        val dfLower = interval.lowerBound match {
          case Unbound() =>
            df
          case Open(x) =>
            df.filter(col > Timestamp.from(x))
          case Closed(x) =>
            df.filter(col >= Timestamp.from(x))
          case _ =>
            throw new RuntimeException(s"Unexpected: $interval")
        }

        interval.upperBound match {
          case Unbound() =>
            dfLower
          case Open(x) =>
            dfLower.filter(col < Timestamp.from(x))
          case Closed(x) =>
            dfLower.filter(col <= Timestamp.from(x))
          case _ =>
            throw new RuntimeException(s"Unexpected: $interval")
        }
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
    inputSlices: Map[DatasetID, InputDataSlice],
    prevCheckpointDir: Option[Path]
  ): Map[DatasetID, Instant] = {
    val previousWatermarks: Map[DatasetID, Instant] =
      if (prevCheckpointDir.isDefined) {
        inputSlices.keys
          .map(id => (id, readWatermark(id, prevCheckpointDir.get)))
          .toMap
      } else {
        Map.empty
      }

    inputSlices.map {
      case (id, slice) =>
        (
          id,
          maxOption(slice.explicitWatermarks.map(_.eventTime))
            .getOrElse(previousWatermarks(id))
        )
    }
  }

  private def readWatermark(
    datasetID: DatasetID,
    checkpointDir: Path
  ): Instant = {
    val wmPath = checkpointDir.resolve(s"$datasetID.watermark")
    val reader = new Scanner(File(wmPath).newInputStream)
    val watermark = Instant.parse(reader.nextLine())
    reader.close()
    watermark
  }

  private def writeWatermark(
    datasetID: DatasetID,
    checkpointDir: Path,
    watermark: Instant
  ): Unit = {
    val outputStream = File(checkpointDir.resolve(s"$datasetID.watermark")).newOutputStream
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

  private def computeHash(df: DataFrame): String = {
    if (df.isEmpty)
      return zero_hash
    new DataFrameDigestSHA256().digest(df)
  }

  private def typedMap[T](m: Map[String, T]): Map[DatasetID, T] = {
    m.map {
      case (id, value) => (DatasetID(id), value)
    }
  }

  private def maxOption[T: Ordering](seq: Seq[T]): Option[T] = {
    if (seq.isEmpty)
      None
    else
      Some(seq.max)
  }

}
