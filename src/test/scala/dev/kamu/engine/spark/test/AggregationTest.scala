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
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.funsuite._

case class TickerSummary(
  interval_start: Timestamp,
  interval_end: Timestamp,
  symbol: String,
  low: Long,
  high: Long
)

case class Trade(
  system_time: Timestamp,
  event_time: Timestamp,
  symbol: String,
  quantity: Long
)

case class Holding(
  event_time: Timestamp,
  symbol: String,
  quantity: Long
)

class AggregationTest extends AnyFunSuite with KamuDataFrameSuite {
  import spark.implicits._

  def t(minute: Int) = ts(2000, 1, 1, 0, minute)

  // TODO: Can't get first/last event within a time window
  test("sliding window") {
    val tickersStream = MemoryStream[Ticker](1, spark.sqlContext)
    val tickersDS = tickersStream.toDS()
    tickersDS
      .withWatermark("event_time", "2 minutes")
      .createOrReplaceTempView("tickers")

    val transform =
      spark.sql("""
    select
      window(event_time, "10 minutes").start as interval_start,
      window(event_time, "10 minutes").end as interval_end,
      symbol,
      min(value) as low,
      max(value) as high
    from tickers
    group by window(event_time, "10 minutes"), symbol
    """)

    val query = transform.writeStream
    //.format("console")
    //.option("truncate", "false")
      .format("memory")
      .queryName("result")
      .outputMode(OutputMode.Append())
      .start

    val result = spark.sql("SELECT * FROM result").as[TickerSummary]

    tickersStream.addData(Ticker(t(1), t(0), "A", 10))
    query.processAllAvailable()

    tickersStream.addData(Ticker(t(7), t(6), "A", 11))
    query.processAllAvailable()

    tickersStream.addData(Ticker(t(8), t(5), "A", 12))
    query.processAllAvailable()

    tickersStream.addData(Ticker(t(13), t(11), "A", 12))
    query.processAllAvailable()

    tickersStream.addData(Ticker(t(14), t(13), "A", 13))
    query.processAllAvailable()

    tickersStream.addData(Ticker(t(18), t(17), "A", 15))
    query.processAllAvailable()

    tickersStream.addData(Ticker(t(23), t(22), "A", 13))
    query.processAllAvailable()

    val expected = sc
      .parallelize(
        Seq(
          TickerSummary(t(0), t(10), "A", 10, 12),
          TickerSummary(t(10), t(20), "A", 12, 15)
        )
      )
      .toDS

    assertDatasetEquals(expected, result)
  }

  // TODO: Spark does not support `order by event_time` even with a watermark
  test("running total") {
    val tradesStream = MemoryStream[Trade](1, spark.sqlContext)
    val trades = tradesStream.toDS()
    trades
      .createOrReplaceTempView("trades")

    val transform = spark.sql(
      """
    select
      last(event_time) as event_time,
      symbol,
      sum(quantity) as quantity
    from trades
    group by symbol
    """
    )

    val query = transform.writeStream
    //.format("console")
    //.option("truncate", "false")
      .format("memory")
      .queryName("result2")
      .outputMode(OutputMode.Update())
      .start

    val result = spark.sql("SELECT * FROM result2").as[Holding]

    tradesStream.addData(Trade(t(1), t(0), "A", 100))
    query.processAllAvailable()

    tradesStream.addData(Trade(t(2), t(1), "A", 100))
    query.processAllAvailable()

    tradesStream.addData(Trade(t(3), t(2), "A", -200))
    query.processAllAvailable()

    val expected = sc
      .parallelize(
        Seq(
          Holding(t(0), "A", 100),
          Holding(t(1), "A", 200),
          Holding(t(2), "A", 0)
        )
      )
      .toDS

    assertDatasetEquals(expected, result)
  }

}
