/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark

import java.sql.Timestamp
import com.holdenkarau.spark.testing.DatasetSuiteBase
import dev.kamu.engine.spark.KamuDataFrameSuite
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.FunSuite

case class Population(
  system_time: Timestamp,
  year: Int,
  city: String,
  population: Int
)

object Population {
  def apply(year: Int, city: String, population: Int): Population =
    Population(
      new Timestamp(System.currentTimeMillis()),
      year,
      city,
      population
    )
}

case class Budget(
  system_time: Timestamp,
  year: Int,
  city: String,
  budget: Int
)

object Budget {
  def apply(year: Int, city: String, budget: Int): Budget =
    Budget(
      new Timestamp(System.currentTimeMillis()),
      year,
      city,
      budget
    )
}

case class Combined(
  year: Int,
  city: String,
  population: Int,
  budget: Int
)

class TemporalJoinTest
    extends FunSuite
    with KamuDataFrameSuite
    with DatasetSuiteBase {
  import spark.implicits._

  test("temporalJoin") {
    val populationsStream = MemoryStream[Population](1, spark.sqlContext)
    populationsStream.toDS().createOrReplaceTempView("populations")

    val budgetsStream = MemoryStream[Budget](2, spark.sqlContext)
    budgetsStream.toDS().createOrReplaceTempView("budgets")

    val transform = spark.sql("""
      SELECT
        current_timestamp() as system_time,
        p.year,
        p.city,
        p.population,
        b.budget
      FROM populations as p
      INNER JOIN budgets as b
      ON p.city = b.city AND p.year = b.year
    """)

    val query = transform.writeStream
      .format("memory")
      .queryName("result")
      .outputMode(OutputMode.Append())
      .start

    val result = spark.sql("SELECT * FROM result")

    def break(): Unit = {
      // Randomization ensures that resulting data does not depend on how batches are split up
      if (scala.util.Random.nextDouble() < 0.3)
        query.processAllAvailable()
    }

    budgetsStream.addData(Budget(2017, "vancouver", 1322))
    break()

    populationsStream.addData(Population(2017, "vancouver", 675218))
    break()

    populationsStream.addData(Population(2017, "seattle", 724745))
    break()

    budgetsStream.addData(Budget(2017, "seattle", 5600))
    break()

    // Correction
    // TODO: how to distinguish correction in the result?
    populationsStream.addData(Population(2017, "vancouver", 675220))
    break()

    populationsStream.addData(Population(2018, "seattle", 731200))
    budgetsStream.addData(Budget(2018, "vancouver", 1407))
    budgetsStream.addData(Budget(2018, "seattle", 5700))
    break()

    populationsStream.addData(Population(2019, "vancouver", 700920))
    populationsStream.addData(Population(2019, "seattle", 745010))
    budgetsStream.addData(Budget(2019, "vancouver", 1513))
    budgetsStream.addData(Budget(2019, "seattle", 5900))
    break()

    // Late arrival
    populationsStream.addData(Population(2018, "vancouver", 690150))

    // Final
    query.processAllAvailable()

    val expected = sc
      .parallelize(
        Seq(
          Combined(2017, "vancouver", 675218, 1322),
          Combined(2017, "seattle", 724745, 5600),
          Combined(2017, "vancouver", 675220, 1322),
          Combined(2018, "seattle", 731200, 5700),
          Combined(2019, "vancouver", 700920, 1513),
          Combined(2019, "seattle", 745010, 5900),
          Combined(2018, "vancouver", 690150, 1407)
        )
      )
      .toDS()
      .orderBy("year", "city", "population")

    val actual = result
      .orderBy("system_time")
      .drop("system_time")
      .orderBy("year", "city", "population")
      .as[Combined]

    assertDatasetEquals(expected, actual)
  }
}
