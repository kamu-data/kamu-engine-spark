/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.transform.streaming

import java.sql.Timestamp

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.FunSuite

class TemporalJoinTest extends FunSuite with DataFrameSuiteBaseEx {
  import spark.implicits._

  protected override val enableHiveSupport = false

  def now(): Timestamp = {
    new Timestamp(System.currentTimeMillis())
  }

  def ts(year: Int = 0, month: Int = 1, day: Int = 1): Timestamp =
    Timestamp.valueOf(f"2$year%03d-$month%02d-$day%02d 00:00:00")

  test("temporalJoin") {
    val populationsMem =
      MemoryStream[String](1, spark.sqlContext)(Encoders.STRING)
    val populations = populationsMem
      .toDF()
      .selectExpr(
        "cast(split(value, ',')[0] as timestamp) as systemTime",
        "cast(split(value, ',')[1] as integer) as year",
        "split(value, ',')[2] as city",
        "cast(split(value, ',')[3] as integer) as population"
      )
    populations.createOrReplaceTempView("populations")

    def addPopulation(year: Integer, city: String, population: Int): Unit = {
      val data = Seq(
        now().toString,
        year.toString,
        city,
        population.toString
      ).mkString(",")
      //println("Adding population record: " + data)
      populationsMem.addData(data)
    }

    val budgetsMem = MemoryStream[String](2, spark.sqlContext)(Encoders.STRING)
    val budgets = budgetsMem
      .toDF()
      .selectExpr(
        "cast(split(value, ',')[0] as timestamp) as systemTime",
        "cast(split(value, ',')[1] as integer) as year",
        "split(value, ',')[2] as city",
        "cast(split(value, ',')[3] as integer) as budget"
      )
    budgets.createOrReplaceTempView("budgets")

    def addBudget(year: Int, city: String, budget: Int): Unit = {
      val data = Seq(
        now().toString,
        year.toString,
        city,
        budget.toString
      ).mkString(",")
      //println("Adding budget record: " + data)
      budgetsMem.addData(data)
    }

    val transform =
      spark.sql(
        """
      SELECT 
        current_timestamp() as systemTime,
        p.year,
        p.city,
        p.population,
        b.budget
      FROM populations as p
      INNER JOIN budgets as b
      ON p.city = b.city AND p.year = b.year
    """
      )

    val query = transform.writeStream
      .format("memory")
      .queryName("result")
      .outputMode(OutputMode.Append())
      .start

    val result = spark.sql("SELECT * FROM result")

    def processStream(): Unit = {
      // Randomization ensures that resulting data does not depend on how batches are split up
      if (scala.util.Random.nextDouble() < 0.3)
        query.processAllAvailable()
    }

    addBudget(2017, "vancouver", 1322)
    processStream()

    addPopulation(2017, "vancouver", 675218)
    processStream()

    addPopulation(2017, "seattle", 724745)
    processStream()

    addBudget(2017, "seattle", 5600)
    processStream()

    // Correction
    addPopulation(2017, "vancouver", 675220)
    processStream()

    addPopulation(2018, "seattle", 731200)
    addBudget(2018, "vancouver", 1407)
    addBudget(2018, "seattle", 5700)
    processStream()

    addPopulation(2019, "vancouver", 700920)
    addPopulation(2019, "seattle", 745010)
    addBudget(2019, "vancouver", 1513)
    addBudget(2019, "seattle", 5900)
    processStream()

    // Late arrival
    addPopulation(2018, "vancouver", 690150)

    // Final
    query.processAllAvailable()

    val expected = sc
      .parallelize(
        Seq(
          (2017, "vancouver", 675218, 1322),
          (2017, "seattle", 724745, 5600),
          (2017, "vancouver", 675220, 1322),
          (2018, "seattle", 731200, 5700),
          (2019, "vancouver", 700920, 1513),
          (2019, "seattle", 745010, 5900),
          (2018, "vancouver", 690150, 1407)
        )
      )
      .toDF("year", "city", "population", "budget")

    val actual = result

    assertDataFrameEquals(
      expected.orderBy("year", "city", "population"),
      actual
        .drop("systemTime")
        .orderBy("year", "city", "population"),
      true
    )
  }
}
