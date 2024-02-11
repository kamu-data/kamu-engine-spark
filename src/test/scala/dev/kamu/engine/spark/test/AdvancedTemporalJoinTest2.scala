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
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.funsuite._

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

class AdvancedTemporalJoinTest2 extends AnyFunSuite with KamuDataFrameSuite {
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
