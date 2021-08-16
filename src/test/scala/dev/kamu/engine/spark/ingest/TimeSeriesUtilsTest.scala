/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest

import dev.kamu.engine.spark.KamuDataFrameSuite
import dev.kamu.engine.spark.ingest.utils.TimeSeriesUtils
import org.scalatest.FunSuite

class TimeSeriesUtilsTest extends FunSuite with KamuDataFrameSuite {
  import spark.implicits._

  def testSeries =
    sc.parallelize(
        Seq(
          (ts(0), "I", 1, "A", "x"),
          (ts(0), "I", 2, "B", "y"),
          (ts(0), "I", 3, "C", "z"),
          (ts(1), "U", 1, "A", "a"),
          (ts(1), "U", 2, "B", "b"),
          (ts(2), "D", 1, "A", "a"),
          (ts(2), "U", 2, "B", "bb"),
          (ts(3), "I", 4, "D", "d")
        )
      )
      .toDF("event_time", "observed", "id", "name", "data")

  test("asOf latest") {
    val series = testSeries

    val actual = TimeSeriesUtils
      .asOf(
        series,
        Seq("id"),
        None,
        "event_time",
        "observed",
        "D"
      )
      .orderBy("id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(2), "U", 2, "B", "bb"),
          (ts(0), "I", 3, "C", "z"),
          (ts(3), "I", 4, "D", "d")
        )
      )
      .toDF("event_time", "observed", "id", "name", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("asOf specific") {
    val series = testSeries

    val actual = TimeSeriesUtils
      .asOf(
        series,
        Seq("id"),
        Some(ts(1)),
        "event_time",
        "observed",
        "D"
      )
      .orderBy("id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(1), "U", 1, "A", "a"),
          (ts(1), "U", 2, "B", "b"),
          (ts(0), "I", 3, "C", "z")
        )
      )
      .toDF(
        "event_time",
        "observed",
        "id",
        "name",
        "data"
      )

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("asOf compound key") {
    val series = sc
      .parallelize(
        Seq(
          (ts(0), "I", 1, "A", "x"),
          (ts(0), "I", 1, "B", "y"),
          (ts(0), "I", 2, "C", "z"),
          (ts(1), "U", 1, "A", "a"),
          (ts(1), "U", 1, "B", "b"),
          (ts(2), "D", 1, "A", "a"),
          (ts(2), "U", 1, "B", "bb"),
          (ts(3), "I", 2, "D", "d")
        )
      )
      .toDF(
        "event_time",
        "observed",
        "key",
        "name",
        "data"
      )

    val actual = TimeSeriesUtils
      .asOf(
        series,
        Seq("key", "name"),
        None,
        "event_time",
        "observed",
        "D"
      )
      .orderBy("key", "name")

    val expected = sc
      .parallelize(
        Seq(
          (ts(2), "U", 1, "B", "bb"),
          (ts(0), "I", 2, "C", "z"),
          (ts(3), "I", 2, "D", "d")
        )
      )
      .toDF(
        "event_time",
        "observed",
        "key",
        "name",
        "data"
      )

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

}
