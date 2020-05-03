/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest.merge

import dev.kamu.core.manifests.DatasetVocabulary
import dev.kamu.core.utils.Clock
import org.apache.spark.sql.DataFrame

/** Ledger merge strategy.
  *
  * This strategy should be used for data dumps containing append-only event
  * streams. New data dumps can have new rows added, but once data already
  * made it into one dump it never changes or disappears.
  *
  * A system time column will be added to the data to indicate the time
  * when the record was observed first by the system.
  *
  * It relies on a user-specified primary key column to identify which records
  * were already seen and not duplicate them.
  *
  * It will always preserve all columns from existing and new snapshots, so
  * the set of columns can only grow.
  *
  * @param pk primary key column name
  */
class LedgerMergeStrategy(
  pk: Vector[String],
  systemClock: Clock,
  vocab: DatasetVocabulary = DatasetVocabulary()
) extends MergeStrategy(systemClock, vocab) {

  override def merge(
    prevRaw: Option[DataFrame],
    currRaw: DataFrame
  ): DataFrame = {
    val (prev, curr, _, _) = prepare(prevRaw, currRaw)

    orderColumns(
      curr
        .join(
          prev,
          pk.map(c => curr(c) <=> prev(c)).reduce(_ && _),
          "left_anti"
        )
    )
  }

}
