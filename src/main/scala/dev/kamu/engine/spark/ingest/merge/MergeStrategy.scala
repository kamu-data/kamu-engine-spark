/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest.merge

import java.sql.Timestamp

import dev.kamu.core.manifests
import dev.kamu.core.manifests.DatasetVocabulary
import dev.kamu.core.utils.Clock
import dev.kamu.engine.spark.ingest.utils.DFUtils._
import dev.kamu.engine.spark.ingest.utils.TimeSeriesUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object MergeStrategy {
  def apply(
    kind: manifests.MergeStrategyKind,
    systemClock: Clock,
    eventTime: Option[Timestamp],
    vocab: DatasetVocabulary
  ): MergeStrategy = {
    kind match {
      case _: manifests.MergeStrategyKind.Append =>
        new AppendMergeStrategy(systemClock, vocab)
      case l: manifests.MergeStrategyKind.Ledger =>
        new LedgerMergeStrategy(l.primaryKey, systemClock, vocab)
      case ss: manifests.MergeStrategyKind.Snapshot =>
        val s = ss.withDefaults()
        new SnapshotMergeStrategy(
          primaryKey = s.primaryKey,
          compareColumns = s.compareColumns,
          eventTimeColumn = s.eventTimeColumn.get,
          eventTime = eventTime.getOrElse(systemClock.timestamp()),
          observationColumn = s.observationColumn.get,
          obsvAdded = s.obsvAdded.get,
          obsvChanged = s.obsvChanged.get,
          obsvRemoved = s.obsvRemoved.get,
          systemClock = systemClock,
          vocab = vocab
        )
      case _ =>
        throw new NotImplementedError(s"Unsupported strategy: $kind")
    }
  }
}

abstract class MergeStrategy(
  systemClock: Clock,
  vocab: DatasetVocabulary
) {

  /** Performs merge-in of the data.
    *
    * @param prev data that is already stored in the system (if any)
    * @param curr data to be added
    */
  def merge(
    prev: Option[DataFrame],
    curr: DataFrame
  ): DataFrame

  /** Adds system columns and reconciles schema differences between previously
    * seen and the new data
    */
  protected def prepare(
    prevRaw: Option[DataFrame],
    currRaw: DataFrame
  ): (DataFrame, DataFrame, Set[String], Set[String]) = {
    if (currRaw.getColumn(vocab.systemTimeColumn).isDefined)
      throw new Exception(
        s"Data already contains column: ${vocab.systemTimeColumn}"
      )

    val currWithSysCols = currRaw
      .withColumn(vocab.systemTimeColumn, lit(systemClock.timestamp()))

    val prevOrEmpty = prevRaw.getOrElse(TimeSeriesUtils.empty(currWithSysCols))

    val combinedColumns =
      (prevOrEmpty.columns ++ currWithSysCols.columns).distinct

    val addedColumns =
      combinedColumns.filter(!prevOrEmpty.hasColumn(_)).toSet
    val removedColumns =
      combinedColumns.filter(!currWithSysCols.hasColumn(_)).toSet

    val prev = prevOrEmpty.select(
      combinedColumns.map(
        c =>
          prevOrEmpty
            .getColumn(c)
            .getOrElse(lit(null))
            .as(c)
      ): _*
    )

    val curr = currWithSysCols.select(
      combinedColumns.map(
        c =>
          currWithSysCols
            .getColumn(c)
            .getOrElse(lit(null))
            .as(c)
      ): _*
    )

    (
      prev,
      curr,
      addedColumns,
      removedColumns
    )
  }

  protected def orderColumns(dataFrame: DataFrame): DataFrame = {
    val columns = Seq(
      vocab.systemTimeColumn
    ).filter(dataFrame.getColumn(_).isDefined)
    dataFrame.columnToFront(columns: _*)
  }
}
