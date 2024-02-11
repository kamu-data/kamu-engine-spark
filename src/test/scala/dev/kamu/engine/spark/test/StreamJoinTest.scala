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
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.funsuite._

class StreamJoinTest extends AnyFunSuite with KamuDataFrameSuite {
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
      factMem.addData(Seq(t.toInstant.toString, city).mkString(","))
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
