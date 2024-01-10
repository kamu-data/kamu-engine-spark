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
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.FunSuite

case class OldBoat(
  system_time: Timestamp,
  event_time: Timestamp,
  id: Int,
  title: String,
  price: Int
)

object OldBoat {
  def apply(time: Timestamp, id: Int, title: String, price: Int): OldBoat =
    this(
      new Timestamp(System.currentTimeMillis()),
      time,
      id,
      title,
      price
    )
}

case class NewBoat(
  system_time: Timestamp,
  event_time: Timestamp,
  id: Int,
  make: String,
  model: String,
  price: Int
)

object NewBoat {
  def apply(
    time: Timestamp,
    id: Int,
    make: String,
    model: String,
    price: Int
  ): NewBoat =
    this(
      new Timestamp(System.currentTimeMillis()),
      time,
      id,
      make,
      model,
      price
    )
}

case class MakeModel(
  make: String,
  model: String
)

case class Make(
  make: String,
  frequency: Long
)

//////////////

class AdvancedTemporalJoinTest2 extends FunSuite with KamuDataFrameSuite {
  import spark.implicits._

  def t(minute: Int) = ts(2000, 1, 1, 0, minute)

  ignore("advancedTemporalJoin2") {
    val oldStream = MemoryStream[OldBoat](1, spark.sqlContext)
    val oldDS = oldStream.toDS()
    oldDS.createOrReplaceTempView("old")

    val newBoats = sc
      .parallelize(
        Seq(
          NewBoat(t(1), 1, "Beneteau", "Oceanis 45", 300),
          NewBoat(t(1), 2, "Jeanneau", "64", 500),
          NewBoat(t(2), 3, "Jeanneau", "55 CC", 350),
          NewBoat(t(2), 4, "Beneteau Oceanis", "50", 300), // Bad data
          NewBoat(t(3), 5, "Beneteau", "First 35", 50),
          NewBoat(t(3), 6, "Jeanneau", "Sun Odyssey 45", 250)
        )
      )
      .toDS()
    newBoats.createOrReplaceTempView("new")

    val makes = spark
      .sql("""
      SELECT
        make, count(*) as frequency
      FROM new
      GROUP BY make
    """)
      .as[Make]
      .collect()

    def parseMakeModel: String => MakeModel = { title =>
      println("Call: " + title)
      val spl = title.split(" ")
      MakeModel(spl(0), spl.drop(1).mkString(" "))
    }

    spark.udf.register("parse_make_model", parseMakeModel)

    val oldTransformed =
      spark.sql(
        """
           select system_time, event_time, id, mm.make, mm.model, title, price from (
           select *, parse_make_model(title) as mm from old)
      """
      )

    val query = oldTransformed.writeStream
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .format("console")
      //.queryName("result")
      .outputMode(OutputMode.Update())
      .start

    oldStream.addData(OldBoat(t(1), 1, "Beneteau Oceanis 45", 300))
    oldStream.addData(OldBoat(t(1), 2, "Jeanneau 64", 500))
    query.processAllAvailable()
    oldStream.addData(OldBoat(t(2), 3, "Jeanneau 55 CC", 350))
    query.processAllAvailable()
    oldStream.addData(OldBoat(t(3), 4, "Beneteau First 35", 50))
    query.processAllAvailable()
  }

}
