/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest

import dev.kamu.core.manifests.DatasetVocabulary
import dev.kamu.engine.spark.KamuDataFrameSuite
import org.scalatest.{FunSuite, Matchers}

class WatermarkTest extends FunSuite with KamuDataFrameSuite with Matchers {
  import spark.implicits._

  test("returns max") {
    val df = sc
      .parallelize(
        Seq(
          (ts(1), 1, "a"),
          (ts(3), 2, "b"),
          (ts(2), 3, "c")
        )
      )
      .toDF("event_time", "id", "data")

    val ingest = new Ingest()
    val wm = ingest.getOutputWatermark(
      df,
      None,
      DatasetVocabulary(None, Some("event_time"))
    )

    wm shouldEqual Some(ts(3).toInstant)
  }

  test("prevents nulls") {
    val df = sc
      .parallelize(
        Seq(
          (ts(1), 1, "a"),
          (ts(3), 2, "b"),
          (null, 3, "c")
        )
      )
      .toDF("event_time", "id", "data")

    val ingest = new Ingest()

    assertThrows[RuntimeException] {
      ingest.getOutputWatermark(
        df,
        None,
        DatasetVocabulary(None, Some("event_time"))
      )
    }
  }

  test("requires strict types") {
    val df = sc
      .parallelize(
        Seq(
          (1, 1, "a"),
          (2, 2, "b"),
          (3, 3, "c")
        )
      )
      .toDF("event_time", "id", "data")

    val ingest = new Ingest()

    assertThrows[RuntimeException] {
      ingest.getOutputWatermark(
        df,
        None,
        DatasetVocabulary(None, Some("event_time"))
      )
    }
  }

}
