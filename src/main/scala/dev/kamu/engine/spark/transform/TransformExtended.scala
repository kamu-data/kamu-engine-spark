/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.transform

import java.io.PrintWriter
import java.sql.Timestamp
import java.time.Instant
import java.util.Scanner

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
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{lit, max}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import spire.math.{Empty, Interval}
import spire.math.interval.{Closed, Open, Unbound}

case class InputSlice(dataFrame: DataFrame, interval: Interval[Instant])

/** Houses logic that eventually should be moved to the coordinator side **/
class TransformExtended(
  fileSystem: FileSystem,
  spark: SparkSession,
  systemClock: Clock
) extends Transform(spark) {
  private val logger = LogManager.getLogger(getClass.getName)

  def executeExtended(request: ExecuteQueryRequest): ExecuteQueryResult = {
    if (request.source.transformEngine != "sparkSQL")
      throw new RuntimeException(
        s"Unsupported engine: ${request.source.transformEngine}"
      )

    val transform =
      yaml.load[TransformKind.SparkSQL](request.source.transform.toConfig)

    val resultLayout = request.datasetLayouts(request.datasetID.toString)

    val resultVocab =
      request.datasetVocabs(request.datasetID.toString).withDefaults()

    val inputSlices =
      prepareInputSlices(
        spark,
        typedMap(request.inputSlices),
        typedMap(request.datasetVocabs),
        typedMap(request.datasetLayouts)
      )

    inputSlices.values.foreach(_.dataFrame.cache())

    val result = execute(request.datasetID, inputSlices, transform)
      .orderBy(resultVocab.eventTimeColumn.get)
      .coalesce(1)

    result.cache()

    // Prepare metadata
    val lastWatermark =
      request.inputSlices.values.map(_.explicitWatermarks.map(_.eventTime))

    val block = MetadataBlock(
      prevBlockHash = "",
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
        resultLayout.checkpointsDir
      ),
      inputSlices = request.source.inputs.map(i => {
        val slice = inputSlices(i.id)
        DataSlice(
          hash = computeHash(slice.dataFrame),
          interval = slice.interval,
          numRecords = slice.dataFrame.count()
        )
      })
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
    writeParquet(resultWithSystemTime, resultLayout.dataDir)

    // Release memory
    result.unpersist(true)
    inputSlices.values.foreach(_.dataFrame.unpersist(true))

    ExecuteQueryResult(block = block, dataFileName = None)
  }

  private def prepareInputSlices(
    spark: SparkSession,
    inputSlices: Map[DatasetID, InputDataSlice],
    inputVocabs: Map[DatasetID, DatasetVocabulary],
    inputLayouts: Map[DatasetID, DatasetLayout]
  ): Map[DatasetID, InputSlice] = {
    inputSlices.map({
      case (id, slice) =>
        val inputSlice =
          prepareInputSlice(
            spark,
            id,
            slice,
            inputVocabs(id).withDefaults(),
            inputLayouts(id)
          )
        (id, inputSlice)
    })
  }

  private def prepareInputSlice(
    spark: SparkSession,
    id: DatasetID,
    slice: InputDataSlice,
    vocab: DatasetVocabulary,
    layout: DatasetLayout
  ): InputSlice = {
    // TODO: use schema from metadata
    val df = spark.read
      .parquet(layout.dataDir.toString)
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

    val dataFiles = fileSystem
      .listStatus(tmpOutDir)
      .filter(_.getPath.getName.endsWith(".snappy.parquet"))

    if (dataFiles.length != 1)
      throw new RuntimeException(
        "Unexpected number of files in output directory:\n" + fileSystem
          .listStatus(tmpOutDir)
          .map(_.getPath)
          .mkString("\n")
      )

    val dataFile = dataFiles.head.getPath

    val targetFile = outDir.resolve(
      systemClock.instant().toString.replaceAll("[:.]", "") + ".snappy.parquet"
    )

    fileSystem.rename(dataFile, targetFile)

    fileSystem.delete(tmpOutDir, true)

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

  private def writeLastWatermarks(
    watermarks: Map[DatasetID, Option[Instant]],
    checkpointDir: Path
  ): Unit = {
    fileSystem.mkdirs(checkpointDir)

    watermarks.filter(_._2.isDefined).foreach {
      case (id, wm) =>
        val outputStream =
          fileSystem.create(checkpointDir.resolve(id.toString), true)
        val writer = new PrintWriter(outputStream)

        writer.println(wm.get.toString)

        writer.close()
        outputStream.close()
    }
  }

  private def readLastWatermarks(
    datasetIDs: Seq[DatasetID],
    checkpointDir: Path
  ): Map[DatasetID, Option[Instant]] = {
    datasetIDs
      .map(id => {
        val wmPath = checkpointDir.resolve(id.toString)
        if (!fileSystem.exists(wmPath)) {
          (id, None)
        } else {
          val reader = new Scanner(fileSystem.open(wmPath))
          val watermark = Instant.parse(reader.nextLine())
          reader.close()
          (id, Some(watermark))
        }
      })
      .toMap
  }

  private def computeHash(df: DataFrame): String = {
    if (df.isEmpty)
      return ""
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
