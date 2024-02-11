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

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.funsuite._

class JoinTest extends AnyFunSuite with KamuDataFrameSuite {
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
