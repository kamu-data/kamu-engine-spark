/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark

object Op {
  val Append: Int = 0
  val Retract: Int = 1
  val CorrectFrom: Int = 2
  val CorrectTo: Int = 3
}
