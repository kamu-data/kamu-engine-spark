/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.transform

case class TransformDef(
  kind: String,
  engine: String,
  version: Option[String],
  /** Processing steps that shape the data */
  queries: Vector[TransformDef.Query] = Vector.empty,
  /** Convenience way to provide a single SQL statement with no alias **/
  query: Option[String]
)

object TransformDef {
  case class Query(
    /** An alias given to the result of this step that can be used to referred to it in the later steps.
      * Acts as a shorthand for `CREATE TEMPORARY VIEW <alias> AS (<query>)`.
      */
    alias: Option[String] = None,
    /** An SQL statement **/
    query: String
  )
}
