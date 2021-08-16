/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark

import java.sql.Timestamp
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
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
      .set("spark.sql.session.timeZone", "UTC")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    SedonaSQLRegistrator.registerAll(spark)
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
