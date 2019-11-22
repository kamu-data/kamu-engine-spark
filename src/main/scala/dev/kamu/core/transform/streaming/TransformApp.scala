/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.transform.streaming

import org.apache.log4j.LogManager

object TransformApp {
  def main(args: Array[String]) {
    val logger = LogManager.getLogger(getClass.getName)
    val config = AppConfig.load()
    if (config.tasks.isEmpty) {
      logger.warn("No tasks specified")
    } else {
      val transform = new Transform(config)
      transform.transform()
    }
  }
}
