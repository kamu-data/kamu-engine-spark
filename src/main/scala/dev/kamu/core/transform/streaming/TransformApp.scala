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
