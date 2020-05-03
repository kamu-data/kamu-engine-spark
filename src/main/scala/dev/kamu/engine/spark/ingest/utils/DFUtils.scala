/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest.utils

import org.apache.spark.sql.{
  Column,
  DFHelper,
  DataFrame,
  RelationalGroupedDataset
}

object DFUtils {

  implicit class DataFrameEx(val df: DataFrame) {

    def showString(
      numRows: Int = 20,
      truncate: Int = 20,
      vertical: Boolean = false
    ): String = {
      DFHelper.showString(df, numRows, truncate, vertical)
    }

    def maybeTransform(
      condition: Boolean,
      tr: DataFrame => DataFrame
    ): DataFrame = {
      if (condition)
        df.transform(tr)
      else
        df
    }

    /** Get column if exists
      *
      * TODO: Will not work with nested columns
      */
    def getColumn(column: String): Option[Column] = {
      if (df.columns.contains(column))
        Some(df(column))
      else
        None
    }

    def hasColumn(column: String): Boolean = {
      df.getColumn(column).isDefined
    }

    /** Reorders columns in the [[DataFrame]] */
    def columnToFront(columns: String*): DataFrame = {
      val front = columns.toList
      val back = df.columns.filter(!front.contains(_))
      val newColumns = front ++ back
      val head :: tail = newColumns
      df.select(head, tail: _*)
    }

    /** Reorders columns in the [[DataFrame]] */
    def columnToBack(columns: String*): DataFrame = {
      val back = columns.toList
      val front = df.columns.filter(!back.contains(_)).toList
      val newColumns = front ++ back
      val head :: tail = newColumns
      df.select(head, tail: _*)
    }
  }

  implicit class RelationalGroupedDatasetEx(val df: RelationalGroupedDataset) {

    /** Fixes variadic argument passing */
    def aggv(columns: Column*): DataFrame = {
      val head :: tail = columns
      df.agg(head, tail: _*)
    }
  }

}
