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
import org.apache.spark.sql.{DataFrame, Encoders}
import org.scalatest.FunSuite

class SCD2JoinTest extends FunSuite with KamuDataFrameSuite {
  import spark.implicits._

  def toSCD2(df: DataFrame): DataFrame = {
    df.selectExpr(
        "*",
        "eventTime as effectiveFrom",
        "LEAD(eventTime, 1, NULL) OVER (PARTITION BY city ORDER BY eventTime) as effectiveTo"
      )
      .drop("eventTime")
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
      .toDF("eventTime", "city", "population")

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
      .toDF("eventTime", "city")
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
        .toDF("eventTime", "city", "population")
    )
    dim.createOrReplaceTempView("dim")

    val result = spark
      .sql("""
      SELECT fact.*, dim.population
      FROM fact
      LEFT JOIN dim
        ON fact.city = dim.city
          AND fact.eventTime >= dim.effectiveFrom
          AND (dim.effectiveTo IS NULL OR fact.eventTime < dim.effectiveTo)
    """)
      .orderBy("eventTime")

    val expected = sc
      .parallelize(
        Seq(
          (ts(1), "A", Some(1)),
          (ts(2), "B", Some(3)),
          (ts(3), "A", Some(4))
        )
      )
      .toDF("eventTime", "city", "population")

    assertDataFrameEquals(expected, result)
  }

  test("SCD2JoinStreamToStatic") {
    val factMem = MemoryStream[String](1, spark.sqlContext)(Encoders.STRING)
    val fact = factMem
      .toDF()
      .selectExpr(
        "cast(split(value, ',')[0] as timestamp) as eventTime",
        "split(value, ',')[1] as city"
      )
    fact.createOrReplaceTempView("fact")

    def addFact(t: Timestamp, city: String): Unit = {
      factMem.addData(Seq(t.toString, city).mkString(","))
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
        .toDF("eventTime", "city", "population")
    )
    dim.createOrReplaceTempView("dim")

    val transform = spark.sql(
      """
      SELECT fact.*, dim.population
      FROM fact
      LEFT JOIN dim
        ON fact.city = dim.city
          AND fact.eventTime >= dim.effectiveFrom
          AND (dim.effectiveTo IS NULL OR fact.eventTime < dim.effectiveTo)
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
      .toDF("eventTime", "city", "population")

    assertDataFrameEquals(expected, result)
  }
}
