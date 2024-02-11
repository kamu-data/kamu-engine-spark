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
import org.apache.spark.sql.{DataFrame, Encoders}
import org.scalatest.funsuite._

class SCD2JoinTest extends AnyFunSuite with KamuDataFrameSuite {
  import spark.implicits._

  def toSCD2(df: DataFrame): DataFrame = {
    df.selectExpr(
        "*",
        "event_time as effectiveFrom",
        "LEAD(event_time, 1, NULL) OVER (PARTITION BY city ORDER BY event_time) as effectiveTo"
      )
      .drop("event_time")
  }

  test("toSCD2") {
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

    val dimSCD2 = toSCD2(dim)
      .orderBy("effectiveFrom", "city")

    val expected = sc
      .parallelize(
        Seq(
          ("A", 1, ts(0), Some(ts(2))),
          ("B", 2, ts(0), Some(ts(1))),
          ("B", 3, ts(1), None),
          ("A", 4, ts(2), Some(ts(4))),
          ("A", 5, ts(4), None)
        )
      )
      .toDF("city", "population", "effectiveFrom", "effectiveTo")

    assertDataFrameEquals(expected, dimSCD2)
  }

  test("SC2JoinStaticToStatic") {
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

    val dim = toSCD2(
      sc.parallelize(
          Seq(
            (ts(0), "A", 1),
            (ts(0), "B", 2),
            (ts(1), "B", 3),
            (ts(2), "A", 4),
            (ts(4), "A", 5)
          )
        )
        .toDF("event_time", "city", "population")
    )
    dim.createOrReplaceTempView("dim")

    val result = spark
      .sql("""
      SELECT fact.*, dim.population
      FROM fact
      LEFT JOIN dim
        ON fact.city = dim.city
          AND fact.event_time >= dim.effectiveFrom
          AND (dim.effectiveTo IS NULL OR fact.event_time < dim.effectiveTo)
    """)
      .orderBy("event_time")

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

  test("SCD2JoinStreamToStatic") {
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

    val dim = toSCD2(
      sc.parallelize(
          Seq(
            (ts(0), "A", 1),
            (ts(0), "B", 2),
            (ts(1), "B", 3),
            (ts(2), "A", 4),
            (ts(4), "A", 5)
          )
        )
        .toDF("event_time", "city", "population")
    )
    dim.createOrReplaceTempView("dim")

    val transform = spark.sql(
      """
      SELECT fact.*, dim.population
      FROM fact
      LEFT JOIN dim
        ON fact.city = dim.city
          AND fact.event_time >= dim.effectiveFrom
          AND (dim.effectiveTo IS NULL OR fact.event_time < dim.effectiveTo)
    """
    )

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
}
