/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest

import dev.kamu.engine.spark.KamuDataFrameSuite
import dev.kamu.engine.spark.ingest.utils.DataFrameDigestSHA256
import org.scalatest.FunSuite

class DataFrameDigestTest extends FunSuite with KamuDataFrameSuite {
  import spark.implicits._

  test("Computes stable digest - simple types") {
    val df = sc
      .parallelize(
        Seq(
          (
            ts(0),
            1,
            "a"
          ),
          (
            ts(1000),
            2,
            "b"
          ),
          (
            ts(2000),
            3,
            "c"
          )
        )
      )
      .toDF("system_time", "id", "name")

    val actual = new DataFrameDigestSHA256().digest(df)

    assert(
      actual == "8c9cff3819cca6c379a0ef03fa786e8b313cbd2047d85950640a321ae58844dc"
    )
  }

  test("Computes stable digest - geometry") {
    val df = sc
      .parallelize(
        Seq(
          (
            ts(0),
            1,
            "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))"
          ),
          (
            ts(1000),
            2,
            "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))"
          ),
          (
            ts(2000),
            3,
            "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))"
          )
        )
      )
      .toDF("system_time", "id", "geom")
      .selectExpr(
        "system_time",
        "id",
        "ST_GeomFromWKT(geom) as geom"
      )

    val actual = new DataFrameDigestSHA256().digest(df)

    assert(
      actual == "8c2110d2bd86a4a1960f9560b63baeacbcabb02748d1df672a7ae8289ebed72c"
    )
  }

  test("Computes stable digest - array of simple types") {
    val df = sc
      .parallelize(
        Seq(
          (
            ts(0),
            1,
            Array("a", "b", "c"),
            Array(3, 2, 1)
          ),
          (
            ts(1000),
            2,
            Array("x", "y"),
            Array(3, 2)
          )
        )
      )
      .toDF("system_time", "id", "tags", "nums")

    val actual = new DataFrameDigestSHA256().digest(df)

    assert(
      actual == "5d09c502c461b53bcd0f1dd5003ea36c801cab311dce8760c70ac5d8a233b4fc"
    )
  }

  test("Computes stable digest - array of timestamps") {
    val df = sc
      .parallelize(
        Seq(
          (
            ts(0),
            1,
            Array(ts(0), ts(1000))
          ),
          (
            ts(1000),
            2,
            Array(ts(2000))
          )
        )
      )
      .toDF("system_time", "id", "times")

    assertThrows[NotImplementedError] {
      new DataFrameDigestSHA256().digest(df)
    }
  }

  test("Computes stable digest - struct of simple types") {
    val df = sc
      .parallelize(
        Seq(
          (
            ts(0),
            1,
            "a",
            1,
            1.5,
            Array("x", "y")
          ),
          (
            ts(1000),
            2,
            "b",
            2,
            3.14,
            Array("z")
          )
        )
      )
      .toDF("system_time", "id", "name", "num", "share", "tags")
      .selectExpr(
        "system_time",
        "struct(id, name) as identity",
        "struct(num, share, tags) as info"
      )

    val actual = new DataFrameDigestSHA256().digest(df)

    assert(
      actual == "43938e5e35a4de93d9b2d79eb6871e9fd335a6995118ebb4e0751a7f19a27731"
    )
  }

}
