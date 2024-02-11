/*
 * Copyright 2018 kamu.dev
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.kamu.engine.spark.test

import java.sql.Timestamp
import org.scalatest.funsuite._

case class Ticker(
  system_time: Timestamp,
  event_time: Timestamp,
  symbol: String,
  value: Long
)

class ProjectionTest extends AnyFunSuite with KamuDataFrameSuite {
  import spark.implicits._

  private def t(minute: Int) = ts(2000, 1, 1, 0, minute)

  private def testProjection(sql: String) {
    val tickers1 = sc
      .parallelize(
        Seq(
          Ticker(t(1), t(0), "A", 100),
          Ticker(t(1), t(0), "B", 300),
          Ticker(t(1), t(0), "C", 200),
          //
          Ticker(t(3), t(2), "A", 102),
          Ticker(t(3), t(2), "B", 302),
          Ticker(t(3), t(2), "C", 202),
          // backfill
          Ticker(t(4), t(1), "A", 101),
          Ticker(t(4), t(1), "B", 301),
          Ticker(t(4), t(1), "C", 201)
        )
      )
      .toDS()
    tickers1.createOrReplaceTempView("tickers")

    val proj1 = spark
      .sql(sql)
      .as[Ticker]
      .orderBy("symbol")

    val projExpected1 = sc
      .parallelize(
        Seq(
          Ticker(t(3), t(2), "A", 102),
          Ticker(t(3), t(2), "B", 302),
          Ticker(t(3), t(2), "C", 202)
        )
      )
      .toDS()

    assertDatasetEquals(projExpected1, proj1)
  }

  test("windowing based") {
    testProjection(
      """
    select
      system_time,
      event_time,
      symbol,
      value
    from (
      select
      *,
      row_number() over(partition by symbol order by event_time desc) as rank
      from tickers
    ) where rank = 1
    """
    )
  }

  test("join based") {
    testProjection(
      """
      with last_events as (
        select distinct
          symbol,
          max(event_time) as event_time
        from tickers
        group by symbol
      )
      select
        t.system_time as system_time,
        t.event_time as event_time,
        t.symbol as symbol,
        t.value as value
      from tickers as t
      join last_events as e
      on t.symbol = e.symbol and t.event_time = e.event_time
    """
    )
  }

}
