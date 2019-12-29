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
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.FunSuite

class StreamJoinTest extends FunSuite with KamuDataFrameSuite {
  import spark.implicits._

  test("eventTimeJoinStaticToStatic") {
    val fact = sc
      .parallelize(
        Seq(
          (ts(1), "A"),
          (ts(2), "B"),
          (ts(3), "A")
        )
      )
      .toDF("event_time", "city")
    fact.createOrReplaceTempView("fact")

    val dim = sc
      .parallelize(
        Seq(
          (ts(0), "A", 1),
          (ts(0), "B", 2),
          (ts(1), "B", 3),
          (ts(2), "A", 4),
          (ts(4), "A", 5)
        )
      )
      .toDF("event_time", "city", "population")
    dim.createOrReplaceTempView("dim")

    val result = spark.sql("""
      SELECT fact.event_time, fact.city, dim.population
      FROM fact
      LEFT JOIN dim
        ON fact.city = dim.city
          AND dim.event_time <= fact.event_time
      LEFT JOIN dim as dim2
        ON fact.city = dim2.city
          AND dim2.event_time <= fact.event_time
          AND dim2.event_time > dim.event_time
      WHERE dim2.event_time IS NULL
    """).orderBy("event_time")

    /* Before WHERE d2.event_time IS NULL
    +-------------------+----+----------+-------------------+-------------------+
    |         event_time|city|population|                 d1|                 d2|
    +-------------------+----+----------+-------------------+-------------------+
    |2001-01-01 00:00:00|   A|         1|2000-01-01 00:00:00|               null|
    |2002-01-01 00:00:00|   B|         2|2000-01-01 00:00:00|2001-01-01 00:00:00|
    |2002-01-01 00:00:00|   B|         3|2001-01-01 00:00:00|               null|
    |2003-01-01 00:00:00|   A|         1|2000-01-01 00:00:00|2002-01-01 00:00:00|
    |2003-01-01 00:00:00|   A|         4|2002-01-01 00:00:00|               null|
    +-------------------+----+----------+-------------------+-------------------+
     */

    val expected = sc
      .parallelize(
        Seq(
          (ts(1), "A", Some(1)),
          (ts(2), "B", Some(3)),
          (ts(3), "A", Some(4))
        )
      )
      .toDF("event_time", "city", "population")

    assertDataFrameEquals(expected, result)
  }

  test("eventTimeJoinStreamToStatic") {
    val factMem = MemoryStream[String](1, spark.sqlContext)(Encoders.STRING)
    val fact = factMem
      .toDF()
      .selectExpr(
        "cast(split(value, ',')[0] as timestamp) as event_time",
        "split(value, ',')[1] as city"
      )
    fact.createOrReplaceTempView("fact")

    def addFact(t: Timestamp, city: String): Unit = {
      factMem.addData(Seq(t.toString, city).mkString(","))
    }

    val dim = sc
      .parallelize(
        Seq(
          (ts(0), "A", 1),
          (ts(0), "B", 2),
          (ts(1), "B", 3),
          (ts(2), "A", 4),
          (ts(4), "A", 5)
        )
      )
      .toDF("event_time", "city", "population")
    dim.createOrReplaceTempView("dim")

    val transform = spark.sql("""
      SELECT fact.event_time, fact.city, dim.population
      FROM fact
      LEFT JOIN dim
        ON fact.city = dim.city
          AND dim.event_time <= fact.event_time
      LEFT JOIN dim as dim2
        ON fact.city = dim2.city
          AND dim2.event_time <= fact.event_time
          AND dim2.event_time > dim.event_time
      WHERE dim2.event_time IS NULL
    """)

    val query = transform.writeStream
      .format("memory")
      .queryName("result")
      .outputMode(OutputMode.Update())
      .start

    val result = spark.sql("SELECT * FROM result")

    query.processAllAvailable()

    addFact(ts(1), "A")
    query.processAllAvailable()

    addFact(ts(2), "B")
    query.processAllAvailable()

    addFact(ts(3), "A")
    query.processAllAvailable()

    query.processAllAvailable()

    val expected = sc
      .parallelize(
        Seq(
          (ts(1), "A", Some(1)),
          (ts(2), "B", Some(3)),
          (ts(3), "A", Some(4))
        )
      )
      .toDF("event_time", "city", "population")

    assertDataFrameEquals(expected, result)
  }

  ignore("eventTimeJoinStreamToStream") {
    val factMem = MemoryStream[String](1, spark.sqlContext)(Encoders.STRING)
    val fact = factMem
      .toDF()
      .selectExpr(
        "cast(split(value, ',')[0] as timestamp) as event_time",
        "split(value, ',')[1] as city"
      )
      .withWatermark("event_time", "1 minute")
    fact.createOrReplaceTempView("fact")

    def addFact(t: Timestamp, city: String): Unit = {
      factMem.addData(Seq(t.toString, city).mkString(","))
    }

    val dimMem = MemoryStream[String](1, spark.sqlContext)(Encoders.STRING)
    val dim = dimMem
      .toDF()
      .selectExpr(
        "cast(split(value, ',')[0] as timestamp) as event_time",
        "split(value, ',')[1] as city",
        "cast(split(value, ',')[1] as int) as population"
      )
      .withWatermark("event_time", "1 minute")
    dim.createOrReplaceTempView("dim")

    def addDim(t: Timestamp, city: String, population: Int): Unit = {
      dimMem.addData(Seq(t.toString, city, population.toString).mkString(","))
    }

    val transform = spark.sql("""
      SELECT fact.event_time, fact.city, dim.population
      FROM fact
      LEFT JOIN dim
        ON fact.city = dim.city
          AND dim.event_time <= fact.event_time
      LEFT JOIN dim as dim2
        ON fact.city = dim2.city
          AND dim2.event_time <= fact.event_time
          AND dim2.event_time > dim.event_time
      WHERE dim2.event_time IS NULL
    """)

    val query = transform.writeStream
      .format("memory")
      .queryName("result")
      .outputMode(OutputMode.Update())
      .start

    val result = spark.sql("SELECT * FROM result")

    addDim(ts(0), "A", 1)
    addDim(ts(0), "B", 2)
    query.processAllAvailable()

    addDim(ts(1), "B", 3)
    addFact(ts(1), "A")
    query.processAllAvailable()

    addDim(ts(2), "A", 4)
    addFact(ts(2), "B")
    query.processAllAvailable()

    addFact(ts(3), "A")
    query.processAllAvailable()

    addDim(ts(4), "A", 5)
    query.processAllAvailable()

    val expected = sc
      .parallelize(
        Seq(
          (ts(1), "A", Some(1)),
          (ts(2), "B", Some(3)),
          (ts(3), "A", Some(4))
        )
      )
      .toDF("event_time", "city", "population")

    assertDataFrameEquals(expected, result)
  }
}
