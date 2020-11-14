/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.transform

import java.nio.file.{Path, Paths}
import java.sql.Timestamp
import java.time.Instant
import java.util.Scanner

import better.files.File
import com.typesafe.config.ConfigObject
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

/** Houses logic that eventually should be moved to the coordinator side **/
class TransformExtended(
  spark: SparkSession,
  systemClock: Clock
) extends Transform(spark) {
  private val logger = LogManager.getLogger(getClass.getName)
  private val zero_hash = "0000000000000000000000000000000000000000000000000000000000000000"

  def executeExtended(request: ExecuteQueryRequest): ExecuteQueryResult = {
    val transform = loadTransform(request.source.transform)

    val resultCheckpointsDir = Paths.get(request.checkpointsDir)
    val resultDataDir = Paths.get(request.dataDirs(request.datasetID.toString))

    val resultVocab =
      request.datasetVocabs(request.datasetID.toString).withDefaults()

    val inputSlices =
      prepareInputSlices(
        spark,
        typedMap(request.inputSlices),
        typedMap(request.datasetVocabs),
        typedMap(request.dataDirs.mapValues(s => Paths.get(s)))
      )

    inputSlices.values.foreach(_.dataFrame.cache())

    val result = execute(request.datasetID, inputSlices, transform)
      .orderBy(resultVocab.eventTimeColumn.get)
      .coalesce(1)

    result.cache()

    // Prepare metadata
    val block = MetadataBlock(
      blockHash = zero_hash,
      prevBlockHash = None,
      systemTime = systemClock.instant(),
      outputSlice = Some(
        DataSlice(
          hash = computeHash(result),
          interval =
            if (result.isEmpty) Interval.empty
            else Interval.point(systemClock.instant()),
          numRecords = result.count()
        )
      ),
      outputWatermark = getLastWatermark(
        typedMap(request.inputSlices),
        resultCheckpointsDir
      ),
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

    // Write data
    writeParquet(resultWithSystemTime, resultDataDir)

    // Release memory
    result.unpersist(true)
    inputSlices.values.foreach(_.dataFrame.unpersist(true))

    ExecuteQueryResult(block = block, dataFileName = None)
  }

  private def prepareInputSlices(
    spark: SparkSession,
    inputSlices: Map[DatasetID, InputDataSlice],
    inputVocabs: Map[DatasetID, DatasetVocabulary],
    inputDataDirs: Map[DatasetID, Path]
  ): Map[DatasetID, InputSlice] = {
    inputSlices.map({
      case (id, slice) =>
        val inputSlice =
          prepareInputSlice(
            spark,
            id,
            slice,
            inputVocabs(id).withDefaults(),
            inputDataDirs(id)
          )
        (id, inputSlice)
    })
  }

  private def prepareInputSlice(
    spark: SparkSession,
    id: DatasetID,
    slice: InputDataSlice,
    vocab: DatasetVocabulary,
    dataDir: Path
  ): InputSlice = {
    // TODO: use schema from metadata
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

  private def writeParquet(df: DataFrame, outDir: Path): Path = {
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

    val targetFile = outDir.resolve(
      systemClock.instant().toString.replaceAll("[:.]", "") + ".snappy.parquet"
    )

    File(dataFile).moveTo(targetFile)
    File(tmpOutDir).delete()

    targetFile
  }

  private def getLastWatermark(
    inputSlices: Map[DatasetID, InputDataSlice],
    checkpointDir: Path
  ): Option[Instant] = {
    // Computes watermark as minimum of all input watermarks

    val previousWatermarks =
      readLastWatermarks(inputSlices.keys.toSeq, checkpointDir)

    val currentWatermarks =
      inputSlices.map {
        case (id, slice) =>
          maxOption(slice.explicitWatermarks.map(_.eventTime))
            .orElse(previousWatermarks(id))
      }

    if (!currentWatermarks.forall(_.isDefined))
      None
    else
      Some(currentWatermarks.flatten.min)
  }

  private def readLastWatermarks(
    datasetIDs: Seq[DatasetID],
    checkpointDir: Path
  ): Map[DatasetID, Option[Instant]] = {
    datasetIDs
      .map(id => {
        val wmPath = checkpointDir.resolve(id.toString)
        if (!File(wmPath).exists) {
          (id, None)
        } else {
          val reader = new Scanner(File(wmPath).newInputStream)
          val watermark = Instant.parse(reader.nextLine())
          reader.close()
          (id, Some(watermark))
        }
      })
      .toMap
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
