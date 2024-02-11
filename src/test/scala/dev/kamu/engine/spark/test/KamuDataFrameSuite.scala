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
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.spark.SedonaContext
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, when}
import org.scalatest.Suite

trait KamuDataFrameSuite extends DatasetSuiteBase { self: Suite =>

  def ts(milis: Long) = new Timestamp(milis)

  def ts(
    year: Int,
    month: Int,
    day: Int,
    hour: Int = 0,
    minute: Int = 0
  ): Timestamp = {
    val dt = ZonedDateTime.of(
      LocalDate.of(year, month, day),
      LocalTime.of(hour, minute),
      ZoneOffset.UTC
    )

    Timestamp.valueOf(dt.toLocalDateTime)
  }

  override def conf: SparkConf = {
    super.conf
      .set("spark.sql.ansi.enabled", "true")
      .set("spark.sql.session.timeZone", "UTC")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val _sedona = SedonaContext.create(spark)
  }

  def toNullableSchema(df: DataFrame): DataFrame = {
    // Coalesce makes spark think the column can be nullable
    df.select(
      df.columns.map(
        c =>
          when(df(c).isNotNull, df(c))
            .otherwise(lit(null))
            .as(c)
      ): _*
    )
  }

  def assertSchemasEqual(
    expected: DataFrame,
    actual: DataFrame,
    ignoreNullable: Boolean
  ): Unit = {
    val exp =
      if (ignoreNullable) toNullableSchema(expected) else expected
    val act =
      if (ignoreNullable) toNullableSchema(actual) else actual
    assert(exp.schema, act.schema)
  }

  def assertDataFrameEquals(
    expected: DataFrame,
    actual: DataFrame,
    ignoreNullable: Boolean
  ): Unit = {
    val exp =
      if (ignoreNullable) toNullableSchema(expected) else expected
    val act =
      if (ignoreNullable) toNullableSchema(actual) else actual
    super.assertDataFrameEquals(exp, act)
  }

}
