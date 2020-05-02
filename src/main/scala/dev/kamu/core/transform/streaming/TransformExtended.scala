/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.transform.streaming

import java.sql.Timestamp
import java.time.Instant

import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.infra.MetadataChainFS
import dev.kamu.core.utils.{Clock, DataFrameDigestSHA256}
import dev.kamu.core.utils.fs._
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import spire.math.{Empty, Interval}
import spire.math.interval.{Closed, Open, Unbound}

case class InputSlice(
  dataFrame: DataFrame,
  dataSlice: DataSlice
)

/** Houses logic that eventually should be moved to the coordinator side **/
class TransformExtended(
  fileSystem: FileSystem,
  spark: SparkSession,
  systemClock: Clock
) extends Transform(fileSystem, spark, systemClock) {
  private val logger = LogManager.getLogger(getClass.getName)

  def executeExtended(task: TransformTaskConfig): Unit = {
    if (task.source.transformEngine != "sparkSQL")
      throw new RuntimeException(
        s"Unsupported engine: ${task.source.transformEngine}"
      )

    val transform =
      yaml.load[TransformKind.SparkSQL](task.source.transform.toConfig)

    val inputSlices =
      prepareInputSlices(
        spark,
        typedMap(task.inputSlices),
        typedMap(task.datasetLayouts)
      )

    val result = execute(task.datasetID, inputSlices, transform)
    result.cache()

    // Compute metadata
    val (resultHash, resultInterval, resultNumRecords) =
      if (!result.isEmpty) {
        (
          computeHash(result),
          Interval.point(systemClock.instant()),
          result.count()
        )
      } else {
        ("", Interval.empty[Instant], 0L)
      }

    // Write data
    result.write
      .mode(SaveMode.Append)
      .parquet(task.datasetLayouts(task.datasetID.toString).dataDir.toString)

    // Release memory
    result.unpersist(true)
    inputSlices.values.foreach(_.dataFrame.unpersist(true))

    val block = MetadataBlock(
      prevBlockHash = "",
      // TODO: Current time? Min of input times? Require to propagate in computations?
      systemTime = systemClock.instant(),
      outputSlice = Some(
        DataSlice(
          hash = resultHash,
          interval = resultInterval,
          numRecords = resultNumRecords
        )
      ),
      inputSlices = task.source.inputs.map(i => inputSlices(i.id).dataSlice)
    )

    val outputStream =
      fileSystem.create(task.metadataOutputDir.resolve("block.yaml"))
    yaml.save(Manifest(block), outputStream)
    outputStream.close()
  }

  private def prepareInputSlices(
    spark: SparkSession,
    inputSlices: Map[DatasetID, DataSlice],
    inputLayouts: Map[DatasetID, DatasetLayout]
  ): Map[DatasetID, InputSlice] = {
    inputSlices.map({
      case (id, slice) =>
        val inputSlice = prepareInputSlice(
          spark,
          id,
          slice,
          inputLayouts(id)
        )
        (id, inputSlice)
    })
  }

  private def prepareInputSlice(
    spark: SparkSession,
    id: DatasetID,
    slice: DataSlice,
    layout: DatasetLayout
  ): InputSlice = {
    val inputMetaChain = new MetadataChainFS(fileSystem, layout.metadataDir)
    val vocab = inputMetaChain
      .getSummary()
      .vocabulary
      .getOrElse(DatasetVocabularyOverrides())
      .asDatasetVocabulary()

    // TODO: use schema from metadata
    val df = spark.read
      .parquet(layout.dataDir.toString)
      .transform(sliceData(slice.interval, vocab))

    df.cache()

    InputSlice(
      dataFrame = df,
      dataSlice = slice.copy(
        hash = computeHash(df),
        numRecords = df.count()
      )
    )
  }

  private def sliceData(interval: Interval[Instant], vocab: DatasetVocabulary)(
    df: DataFrame
  ): DataFrame = {
    interval match {
      case Empty() =>
        df.where(lit(false))
      case _ =>
        val col = df.col(vocab.systemTimeColumn)

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

  private def computeHash(df: DataFrame): String = {
    // TODO: drop system time column first?
    new DataFrameDigestSHA256().digest(df)
  }

  private def typedMap[T](m: Map[String, T]): Map[DatasetID, T] = {
    m.map {
      case (id, value) => (DatasetID(id), value)
    }
  }

}
