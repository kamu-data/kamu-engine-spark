package dev.kamu.core.transform.streaming

import org.apache.log4j.LogManager

object TransformApp {
  def main(args: Array[String]) {
    val logger = LogManager.getLogger(getClass.getName)
    val config = AppConfig.load()
    if (config.transforms.isEmpty) {
      logger.warn("No transformations specified")
    } else {
      val transform = new Transform(config)
      transform.transform()
    }
  }
}
