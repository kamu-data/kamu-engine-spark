/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest.merge

import java.sql.Timestamp

import dev.kamu.engine.spark.ingest.utils.DFUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

/** Append merge strategy.
  *
  * See [[dev.kamu.core.manifests.MergeStrategyKind.Append]] for details.
  */
class AppendMergeStrategy(
  eventTimeColumn: String,
  eventTime: Timestamp
) extends MergeStrategy(eventTimeColumn) {

  override def merge(
    prevRaw: Option[DataFrame],
    currRaw: DataFrame
  ): DataFrame = {
    val currWithEventTime =
      if (currRaw.getColumn(eventTimeColumn).isDefined) {
        currRaw
      } else {
        currRaw.withColumn(eventTimeColumn, lit(eventTime))
      }

    val (_, curr, _, _) = prepare(prevRaw, currWithEventTime)
    orderColumns(curr)
  }

}
