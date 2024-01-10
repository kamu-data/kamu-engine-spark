/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.test

import java.sql.Timestamp
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.FunSuite

case class TickerEvent(
  system_time: Timestamp,
  time: Timestamp,
  symbol: String,
  price: Int
)

object TickerEvent {
  def apply(time: Timestamp, symbol: String, price: Int): TickerEvent =
    this(
      new Timestamp(System.currentTimeMillis()),
      time,
      symbol,
      price
    )
}

case class AssetEvent(
  system_time: Timestamp,
  time: Timestamp,
  operation: String,
  symbol: String,
  delta: Int
)

object AssetEvent {
  def apply(
    time: Timestamp,
    operation: String,
    symbol: String,
    delta: Int
  ): AssetEvent =
    this(
      new Timestamp(System.currentTimeMillis()),
      time,
      operation,
      symbol,
      delta
    )
}

class AdvancedTemporalJoinTest extends FunSuite with KamuDataFrameSuite {
  import spark.implicits._

  ignore("advancedTemporalJoin") {
    val tickerStream = MemoryStream[TickerEvent](1, spark.sqlContext)
    val tickerDS = tickerStream.toDS()
    tickerDS.createOrReplaceTempView("tickers")

    val assetStream = MemoryStream[AssetEvent](2, spark.sqlContext)
    val assetDS = assetStream.toDS()
    assetDS.createOrReplaceTempView("assets")

    val transform = spark.sql("""
      SELECT
        *
      FROM tickers as t
    """)

    val query = transform.writeStream
      .format("console")
      //.queryName("result")
      .outputMode(OutputMode.Append())
      .start

    tickerStream.addData(TickerEvent(ts(2000, 1, 1), "XAU", 1000))
    tickerStream.addData(TickerEvent(ts(2000, 2, 1), "XAU", 1200))
    tickerStream.addData(TickerEvent(ts(2000, 3, 1), "XAU", 1500))

    assetStream.addData(AssetEvent(ts(0, 1, 15), "buy", "XAU", 10))
    assetStream.addData(AssetEvent(ts(0, 2, 1), "buy", "XAU", 10))
    assetStream.addData(AssetEvent(ts(0, 2, 15), "sell", "XAU", 5))
    assetStream.addData(AssetEvent(ts(0, 3, 5), "sell", "XAU", 5))

    // Final
    query.processAllAvailable()
  }
}
