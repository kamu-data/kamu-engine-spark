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
import dev.kamu.engine.spark.ingest.merge.SnapshotMergeStrategy
import org.scalatest.FunSuite

case class Employee(
  id: Int,
  name: String,
  salary: Int
)

case class EmployeeV2(
  id: Int,
  name: String,
  department: String,
  salary: Int
)

case class EmployeeEvent(
  system_time: Timestamp,
  event_time: Timestamp,
  observed: String,
  id: Int,
  name: String,
  salary: Int
)

case class EmployeeEventV2(
  system_time: Timestamp,
  event_time: Timestamp,
  observed: String,
  id: Int,
  name: String,
  department: String,
  salary: Int
)

class MergeStrategySnapshotTest extends FunSuite with KamuDataFrameSuite {
  import spark.implicits._

  protected override val enableHiveSupport = false

  def clockAt(timestamp: Timestamp) = {
    val systemClock = new ManualClock()
    systemClock.set(timestamp)
    systemClock
  }

  test("From empty") {
    val curr = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val t_e = new Timestamp(0)
    val t_s = new Timestamp(1)

    val actual = new SnapshotMergeStrategy(
      primaryKey = Vector("id"),
      systemClock = clockAt(t_s),
      eventTime = t_e
    ).merge(None, curr)
      .as[EmployeeEvent]
      .orderBy("system_time", "event_time", "id")

    val expected = sc
      .parallelize(
        Seq(
          EmployeeEvent(t_s, t_e, "I", 1, "Alice", 100),
          EmployeeEvent(t_s, t_e, "I", 2, "Bob", 80),
          EmployeeEvent(t_s, t_e, "I", 3, "Charlie", 120)
        )
      )
      .toDS
      .orderBy("system_time", "event_time", "id")

    assertDatasetEquals(expected, actual)
  }

  test("No changes") {
    val curr = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val t_e1 = new Timestamp(0)
    val t_s1 = new Timestamp(1)

    val prev = new SnapshotMergeStrategy(
      primaryKey = Vector("id"),
      systemClock = clockAt(t_s1),
      eventTime = t_e1
    ).merge(None, curr)

    val t_e2 = new Timestamp(2)
    val t_s2 = new Timestamp(3)

    val actual = new SnapshotMergeStrategy(
      primaryKey = Vector("id"),
      systemClock = clockAt(t_s2),
      eventTime = t_e2
    ).merge(Some(prev), curr)
      .as[EmployeeEvent]

    assert(actual.isEmpty)
  }

  test("All types of changes") {
    val data1 = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val data2 = sc
      .parallelize(
        Seq(
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 130),
          Employee(4, "Dan", 100)
        )
      )
      .toDF()

    val t_e1 = new Timestamp(0)
    val t_s1 = new Timestamp(1)

    val prev = new SnapshotMergeStrategy(
      primaryKey = Vector("id"),
      systemClock = clockAt(t_s1),
      eventTime = t_e1
    ).merge(None, data1)

    val t_e2 = new Timestamp(2)
    val t_s2 = new Timestamp(3)

    val actual = new SnapshotMergeStrategy(
      primaryKey = Vector("id"),
      systemClock = clockAt(t_s2),
      eventTime = t_e2
    ).merge(Some(prev), data2)
      .as[EmployeeEvent]
      .orderBy("system_time", "event_time", "id")

    val expected = sc
      .parallelize(
        Seq(
          EmployeeEvent(t_s2, t_e2, "D", 1, "Alice", 100),
          EmployeeEvent(t_s2, t_e2, "U", 3, "Charlie", 130),
          EmployeeEvent(t_s2, t_e2, "I", 4, "Dan", 100)
        )
      )
      .toDS()
      .orderBy("system_time", "event_time", "id")

    assertDatasetEquals(expected, actual)
  }

  test("All types of changes with duplicates") {
    val data1 = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val data2 = sc
      .parallelize(
        Seq(
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 130),
          Employee(3, "Charlie", 130),
          Employee(4, "Dan", 100),
          Employee(4, "Dan", 120)
        )
      )
      .toDF()

    val t_e1 = new Timestamp(0)
    val t_s1 = new Timestamp(1)

    val prev = new SnapshotMergeStrategy(
      primaryKey = Vector("id"),
      systemClock = clockAt(t_s1),
      eventTime = t_e1
    ).merge(None, data1)

    val t_e2 = new Timestamp(2)
    val t_s2 = new Timestamp(3)

    val actual = new SnapshotMergeStrategy(
      primaryKey = Vector("id"),
      systemClock = clockAt(t_s2),
      eventTime = t_e2
    ).merge(Some(prev), data2)
      .as[EmployeeEvent]
      .orderBy("system_time", "event_time", "id")

    val expected = sc
      .parallelize(
        Seq(
          EmployeeEvent(t_s2, t_e2, "D", 1, "Alice", 100),
          // Complete duplicate removed
          EmployeeEvent(t_s2, t_e2, "U", 3, "Charlie", 130),
          // On PK duplicate currently selects first occurrence (undefined behavior in general)
          EmployeeEvent(t_s2, t_e2, "I", 4, "Dan", 100)
        )
      )
      .toDS()
      .orderBy("system_time", "event_time", "id")

    assertDatasetEquals(expected, actual)
  }

  test("Does not support event times from the past") {
    val data1 = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val data2 = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val prev = new SnapshotMergeStrategy(
      primaryKey = Vector("id"),
      systemClock = clockAt(ts(2)),
      eventTime = ts(1)
    ).merge(None, data1)

    assertThrows[Exception] {
      new SnapshotMergeStrategy(
        primaryKey = Vector("id"),
        systemClock = clockAt(ts(3)),
        eventTime = ts(0)
      ).merge(
        Some(prev),
        data2
      )
    }
  }

  test("New column added") {
    val data1 = sc
      .parallelize(
        Seq(
          Employee(1, "Alice", 100),
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120)
        )
      )
      .toDF()

    val data2 = sc
      .parallelize(
        Seq(
          EmployeeV2(2, "Bob", "IT", 80),
          EmployeeV2(3, "Charlie", "IT", 130),
          EmployeeV2(4, "Dan", "Accounting", 100)
        )
      )
      .toDF()

    val t_e1 = new Timestamp(0)
    val t_s1 = new Timestamp(1)

    val prev = new SnapshotMergeStrategy(
      primaryKey = Vector("id"),
      systemClock = clockAt(t_s1),
      eventTime = t_e1
    ).merge(None, data1)

    val t_e2 = new Timestamp(2)
    val t_s2 = new Timestamp(3)

    val actual = new SnapshotMergeStrategy(
      primaryKey = Vector("id"),
      systemClock = clockAt(t_s2),
      eventTime = t_e2
    ).merge(Some(prev), data2)
      .as[EmployeeEventV2]
      .orderBy("system_time", "event_time", "id")

    val expected = sc
      .parallelize(
        Seq(
          EmployeeEventV2(t_s2, t_e2, "D", 1, "Alice", null, 100),
          EmployeeEventV2(t_s2, t_e2, "U", 2, "Bob", "IT", 80),
          EmployeeEventV2(t_s2, t_e2, "U", 3, "Charlie", "IT", 130),
          EmployeeEventV2(t_s2, t_e2, "I", 4, "Dan", "Accounting", 100)
        )
      )
      .toDS()
      .orderBy("system_time", "event_time", "id")

    assertDatasetEquals(expected, actual)
  }

  test("Old column missing") {
    val data1 = sc
      .parallelize(
        Seq(
          EmployeeV2(1, "Alice", "IT", 100),
          EmployeeV2(2, "Bob", "IT", 80),
          EmployeeV2(3, "Charlie", "IT", 120)
        )
      )
      .toDF()

    val data2 = sc
      .parallelize(
        Seq(
          Employee(2, "Bob", 80),
          Employee(3, "Charlie", 120),
          Employee(4, "Dan", 100)
        )
      )
      .toDF()

    val t_e1 = new Timestamp(0)
    val t_s1 = new Timestamp(1)

    val prev = new SnapshotMergeStrategy(
      primaryKey = Vector("id"),
      systemClock = clockAt(t_s1),
      eventTime = t_e1
    ).merge(None, data1)

    val t_e2 = new Timestamp(2)
    val t_s2 = new Timestamp(3)

    val actual = new SnapshotMergeStrategy(
      primaryKey = Vector("id"),
      systemClock = clockAt(t_s2),
      eventTime = t_e2
    ).merge(Some(prev), data2)
      .as[EmployeeEventV2]
      .orderBy("system_time", "event_time", "id")

    val expected = sc
      .parallelize(
        Seq(
          EmployeeEventV2(t_s2, t_e2, "D", 1, "Alice", "IT", 100),
          EmployeeEventV2(t_s2, t_e2, "U", 2, "Bob", null, 80),
          EmployeeEventV2(t_s2, t_e2, "U", 3, "Charlie", null, 120),
          EmployeeEventV2(t_s2, t_e2, "I", 4, "Dan", null, 100)
        )
      )
      .toDS()
      .orderBy("system_time", "event_time", "id")

    assertDatasetEquals(expected, actual)
  }
}
