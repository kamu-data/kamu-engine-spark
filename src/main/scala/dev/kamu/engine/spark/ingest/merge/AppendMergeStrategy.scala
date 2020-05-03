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

/** Append merge strategy.
  *
  * See [[dev.kamu.core.manifests.MergeStrategyKind.Append]] for details.
  */
class AppendMergeStrategy(
  systemClock: Clock,
  vocab: DatasetVocabulary = DatasetVocabulary()
) extends MergeStrategy(systemClock, vocab) {

  override def merge(
    prevRaw: Option[DataFrame],
    currRaw: DataFrame
  ): DataFrame = {
    val (_, curr, _, _) = prepare(prevRaw, currRaw)
    orderColumns(curr)
  }

}
