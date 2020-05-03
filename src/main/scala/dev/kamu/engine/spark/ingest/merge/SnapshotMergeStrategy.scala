/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest.merge

import java.sql.Timestamp

import dev.kamu.core.manifests.DatasetVocabulary
import dev.kamu.core.utils.Clock
import dev.kamu.engine.spark.ingest.utils.DFUtils._
import dev.kamu.engine.spark.ingest.utils.TimeSeriesUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Snapshot data merge strategy.
  *
  * See [[dev.kamu.core.manifests.MergeStrategyKind.Snapshot]] for details.
  */
class SnapshotMergeStrategy(
  primaryKey: Vector[String],
  compareColumns: Vector[String] = Vector.empty,
  systemClock: Clock,
  eventTime: Timestamp,
  eventTimeColumn: String = "event_time",
  observationColumn: String = "observed",
  obsvAdded: String = "I",
  obsvChanged: String = "U",
  obsvRemoved: String = "D",
  vocab: DatasetVocabulary = DatasetVocabulary()
) extends MergeStrategy(systemClock, vocab) {

  override def merge(
    prevRaw: Option[DataFrame],
    currRaw: DataFrame
  ): DataFrame = {
    val (prev, curr, addedColumns, removedColumns) = prepare(prevRaw, currRaw)

    ensureEventTimeDoesntGoBackwards(prev, curr)

    val dataColumns = curr.columns
      .filter(
        c => c != vocab.systemTimeColumn && c != eventTimeColumn && c != observationColumn
      )
      .toVector

    val columnsToCompare =
      if (compareColumns.nonEmpty) {
        val invalidColumns = compareColumns.filter(!curr.hasColumn(_))
        if (invalidColumns.nonEmpty)
          throw new RuntimeException(
            s"Column does not exist: " + invalidColumns.mkString(", ")
          )
        compareColumns
      } else {
        dataColumns.filter(!primaryKey.contains(_))
      }

    val prevProj = TimeSeriesUtils.asOf(
      prev,
      primaryKey,
      None,
      eventTimeColumn,
      observationColumn,
      obsvRemoved
    )

    // We consider data changed when
    // either both columns exist and have different values
    // or column disappears while having non-null value
    // or column is added with non-null value
    val changedPredicate = columnsToCompare
      .map(c => {
        if (addedColumns.contains(c)) {
          curr(c).isNotNull
        } else if (removedColumns.contains(c)) {
          prevProj(c).isNotNull
        } else {
          prevProj(c) =!= curr(c)
        }
      })
      .foldLeft(lit(false))((a, b) => a || b)

    val resultDataColumns = dataColumns
      .map(
        c =>
          when(
            col(observationColumn) === obsvRemoved,
            prevProj(c)
          ).otherwise(curr(c))
            .as(c)
      )

    val resultColumns = (
      lit(systemClock.timestamp()).as(vocab.systemTimeColumn)
        :: when(
          col(observationColumn) === obsvRemoved,
          lit(eventTime)
        ).otherwise(curr(eventTimeColumn)).as(eventTimeColumn)
        :: col(observationColumn)
        :: resultDataColumns.toList
    )

    val result = curr
      .drop(vocab.systemTimeColumn, observationColumn)
      .join(
        prevProj,
        primaryKey.map(c => prevProj(c) <=> curr(c)).reduce(_ && _),
        "full_outer"
      )
      .filter(
        primaryKey.map(curr(_).isNull).reduce(_ && _)
          || primaryKey.map(prevProj(_).isNull).reduce(_ && _)
          || changedPredicate
      )
      .withColumn(
        observationColumn,
        when(
          primaryKey.map(prevProj(_).isNull).reduce(_ && _),
          obsvAdded
        ).when(
            primaryKey.map(curr(_).isNull).reduce(_ && _),
            obsvRemoved
          )
          .otherwise(obsvChanged)
      )
      .dropDuplicates(primaryKey)
      .select(resultColumns: _*)

    orderColumns(result)
  }

  def ensureEventTimeDoesntGoBackwards(
    prev: DataFrame,
    curr: DataFrame
  ): Unit = {
    val lastSeenMaxEventTime =
      prev.agg(max(eventTimeColumn)).head().getTimestamp(0)
    val newMinEventTime =
      curr.agg(min(eventTimeColumn)).head().getTimestamp(0)

    if (lastSeenMaxEventTime != null && newMinEventTime != null && lastSeenMaxEventTime
          .compareTo(newMinEventTime) >= 0) {
      throw new Exception(
        s"Past event time was seen, snapshots don't support adding data out of order: $lastSeenMaxEventTime >= $newMinEventTime"
      )
    }
  }

  override protected def prepare(
    prevRaw: Option[DataFrame],
    currRaw: DataFrame
  ): (DataFrame, DataFrame, Set[String], Set[String]) = {
    if (currRaw.hasColumn(eventTimeColumn))
      throw new Exception(s"Data already contains column: $eventTimeColumn")
    if (currRaw.hasColumn(observationColumn))
      throw new Exception(s"Data already contains column: $observationColumn")

    val currWithMergeCols = currRaw
      .withColumn(eventTimeColumn, lit(eventTime))
      .withColumn(observationColumn, lit(obsvAdded))

    super.prepare(prevRaw, currWithMergeCols)
  }

  override protected def orderColumns(dataFrame: DataFrame): DataFrame = {
    val columns = Seq(
      vocab.systemTimeColumn,
      eventTimeColumn,
      observationColumn
    ).filter(dataFrame.getColumn(_).isDefined)
    dataFrame.columnToFront(columns: _*)
  }
}
