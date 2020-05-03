/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest.utils

import java.sql.Timestamp

import DFUtils._
import org.apache.spark.sql.functions.{col, last, lit}
import org.apache.spark.sql.{DataFrame, Row}

object TimeSeriesUtils {

  def empty(proto: DataFrame): DataFrame = {
    val spark = proto.sparkSession

    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], proto.schema)
  }

  /** Projects snapshot history data onto specific point in time.
    *
    * TODO: Test performance of JOIN vs Window function ranking
    **/
  def asOf(
    series: DataFrame,
    primaryKey: Seq[String],
    asOfTime: Option[Timestamp],
    timeCol: String,
    observationColumn: String,
    obsvRemoved: String
  ): DataFrame = {
    val pk = primaryKey.toVector

    def aggAlias(c: String) = "__" + c

    val dataColumns = series.columns
      .filter(!pk.contains(_))
      .toList

    val aggregates = dataColumns
      .map(c => last(c).as(aggAlias(c)))

    val resultColumns = series.columns.map(
      c =>
        if (dataColumns.contains(c))
          col(aggAlias(c)).as(c)
        else
          col(c)
    )

    series
      .when(_ => asOfTime.isDefined)(
        _.filter(col(timeCol) <= asOfTime.get)
      )
      .orderBy(col(timeCol))
      .groupBy(pk.map(col): _*)
      .aggv(aggregates: _*)
      .filter(col(aggAlias(observationColumn)) =!= lit(obsvRemoved))
      .select(resultColumns: _*)
  }

  implicit class When[A](a: A) {
    def when(f: A => Boolean)(g: A => A): A = if (f(a)) g(a) else a
  }

}
