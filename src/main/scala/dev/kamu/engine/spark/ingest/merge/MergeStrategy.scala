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
import dev.kamu.engine.spark.ingest.utils.DFUtils._
import dev.kamu.engine.spark.ingest.utils.TimeSeriesUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object MergeStrategy {
  def apply(
    kind: manifests.MergeStrategy,
    eventTimeColumn: String,
    eventTime: Timestamp
  ): MergeStrategy = {
    kind match {
      case _: manifests.MergeStrategy.Append =>
        new AppendMergeStrategy(
          eventTimeColumn,
          eventTime
        )
      case l: manifests.MergeStrategy.Ledger =>
        new LedgerMergeStrategy(
          eventTimeColumn,
          l.primaryKey
        )
      case ss: manifests.MergeStrategy.Snapshot =>
        val s = ss.withDefaults()
        new SnapshotMergeStrategy(
          primaryKey = s.primaryKey,
          compareColumns = s.compareColumns.getOrElse(Vector.empty),
          eventTimeColumn = eventTimeColumn,
          eventTime = eventTime,
          observationColumn = s.observationColumn.get,
          obsvAdded = s.obsvAdded.get,
          obsvChanged = s.obsvChanged.get,
          obsvRemoved = s.obsvRemoved.get
        )
      case _ =>
        throw new NotImplementedError(s"Unsupported strategy: $kind")
    }
  }
}

abstract class MergeStrategy(eventTimeColumn: String) {

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
    if (currRaw.getColumn(eventTimeColumn).isEmpty)
      throw new Exception(
        s"Event time column $eventTimeColumn was not found amongst: " +
          currRaw.columns.mkString(", ")
      )

    val prevOrEmpty = prevRaw.getOrElse(TimeSeriesUtils.empty(currRaw))

    val combinedColumns =
      (prevOrEmpty.columns ++ currRaw.columns).distinct

    val addedColumns =
      combinedColumns.filter(!prevOrEmpty.hasColumn(_)).toSet
    val removedColumns =
      combinedColumns.filter(!currRaw.hasColumn(_)).toSet

    val prev = prevOrEmpty.select(
      combinedColumns.map(
        c =>
          prevOrEmpty
            .getColumn(c)
            .getOrElse(lit(null))
            .as(c)
      ): _*
    )

    val curr = currRaw.select(
      combinedColumns.map(
        c =>
          currRaw
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
    dataFrame.columnToFront(eventTimeColumn)
  }
}
