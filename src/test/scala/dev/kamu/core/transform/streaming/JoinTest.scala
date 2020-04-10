/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.transform.streaming

import java.sql.Timestamp

import dev.kamu.core.utils.test.KamuDataFrameSuite
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.FunSuite

class JoinTest extends FunSuite with KamuDataFrameSuite {
  import spark.implicits._

  def t(minute: Int) = ts(2000, 1, 1, 0, minute)

  ignore("inner join") {
    val tickersStream = MemoryStream[Ticker](1, spark.sqlContext)
    val tickers = tickersStream.toDS()
    tickers
      .withWatermark("event_time", "2 minutes")
      .createOrReplaceTempView("tickers")

    val holdingsStream = MemoryStream[Holding](2, spark.sqlContext)
    val holdings = holdingsStream.toDS()
    holdings
      .withWatermark("event_time", "2 minutes")
      .createOrReplaceTempView("holdings")

    val result = spark.sql(
      """
    select
      t.event_time as event_time,
      t.symbol as symbol,
      h.quantity,
      h.quantity * t.value as value
    from tickers as t
    join holdings as h
    on t.symbol = h.symbol and t.event_time >= h.event_time
    """
    )

    val query = result.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start

    tickersStream.addData(Ticker(t(1), t(0), "A", 10))
    query.processAllAvailable()

    holdingsStream.addData(Holding(t(1), "A", 100))
    query.processAllAvailable()

    tickersStream.addData(Ticker(t(3), t(2), "A", 11))
    query.processAllAvailable()

    tickersStream.addData(Ticker(t(4), t(3), "A", 12))
    query.processAllAvailable()

    holdingsStream.addData(Holding(t(4), "A", 200))
    tickersStream.addData(Ticker(t(5), t(4), "A", 13))
    query.processAllAvailable()

    tickersStream.addData(Ticker(t(6), t(5), "A", 12))
    query.processAllAvailable()
  }

}
