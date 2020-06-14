/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest

import java.sql.Timestamp

import dev.kamu.core.utils.ManualClock
import dev.kamu.core.utils.test.KamuDataFrameSuite
import dev.kamu.engine.spark.ingest.merge.AppendMergeStrategy
import org.scalatest.FunSuite

class MergeStrategyAppendTest extends FunSuite with KamuDataFrameSuite {
  import spark.implicits._

  protected override val enableHiveSupport = false

  test("From empty - without event time") {
    val curr = sc
      .parallelize(
        Seq(
          (1, "a"),
          (2, "b"),
          (3, "c")
        )
      )
      .toDF("id", "data")

    val strategy = new AppendMergeStrategy("event_time", ts(1))

    val actual = strategy
      .merge(None, curr)
      .orderBy("id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(1), 1, "a"),
          (ts(1), 2, "b"),
          (ts(1), 3, "c")
        )
      )
      .toDF("event_time", "id", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("From empty - with event time") {
    val curr = sc
      .parallelize(
        Seq(
          (ts(0), 1, "a"),
          (ts(1), 2, "b"),
          (ts(2), 3, "c")
        )
      )
      .toDF("event_time", "id", "data")

    val strategy = new AppendMergeStrategy("event_time", ts(1))

    val actual = strategy
      .merge(None, curr)
      .orderBy("event_time", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(0), 1, "a"),
          (ts(1), 2, "b"),
          (ts(2), 3, "c")
        )
      )
      .toDF("event_time", "id", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("Append existing") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(0), 1, "a"),
          (ts(0), 2, "b"),
          (ts(0), 3, "c")
        )
      )
      .toDF("event_time", "id", "data")

    val curr = sc
      .parallelize(
        Seq(
          (ts(1), 4, "d"),
          (ts(1), 5, "e"),
          (ts(1), 6, "f")
        )
      )
      .toDF("event_time", "id", "data")

    val strategy = new AppendMergeStrategy("event_time", ts(2))

    val actual = strategy
      .merge(Some(prev), curr)
      .orderBy("event_time", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(1), 4, "d"),
          (ts(1), 5, "e"),
          (ts(1), 6, "f")
        )
      )
      .toDF("event_time", "id", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  test("New column added") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(0), 1, "a"),
          (ts(0), 2, "b"),
          (ts(0), 3, "c")
        )
      )
      .toDF("event_time", "id", "data")

    val curr = sc
      .parallelize(
        Seq(
          (ts(1), 4, "x", "d"),
          (ts(1), 5, "y", "e"),
          (ts(1), 6, "z", "f")
        )
      )
      .toDF("event_time", "id", "extra", "data")

    val strategy = new AppendMergeStrategy("event_time", ts(2))

    val actual = strategy
      .merge(Some(prev), curr)
      .orderBy("event_time", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(1), 4, "d", "x"),
          (ts(1), 5, "e", "y"),
          (ts(1), 6, "f", "z")
        )
      )
      .toDF("event_time", "id", "data", "extra")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

  // TODO: Consider just returning an error, this is a ledger after all
  test("Old column removed") {
    val prev = sc
      .parallelize(
        Seq(
          (ts(0), 1, "x", "a"),
          (ts(0), 2, "y", "b"),
          (ts(0), 3, "z", "c")
        )
      )
      .toDF("event_time", "id", "extra", "data")

    val curr = sc
      .parallelize(
        Seq(
          (ts(1), 4, "d"),
          (ts(1), 5, "e"),
          (ts(1), 6, "f")
        )
      )
      .toDF("event_time", "id", "data")

    val strategy = new AppendMergeStrategy("event_time", ts(2))

    val actual = strategy
      .merge(Some(prev), curr)
      .orderBy("event_time", "id")

    val expected = sc
      .parallelize(
        Seq(
          (ts(1), 4, null, "d"),
          (ts(1), 5, null, "e"),
          (ts(1), 6, null, "f")
        )
      )
      .toDF("event_time", "id", "extra", "data")

    assertDataFrameEquals(expected, actual, ignoreNullable = true)
  }

}
