/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.transform

import java.io.InputStream

import dev.kamu.core.manifests._
import org.apache.hadoop.fs.Path

case class TransformTaskConfig(
  datasetID: DatasetID,
  source: SourceKind.Derivative,
  inputSlices: Map[String, DataSlice],
  datasetLayouts: Map[String, DatasetLayout],
  metadataOutputDir: Path
) extends Resource

case class AppConfig(
  tasks: Vector[TransformTaskConfig]
) extends Resource

object AppConfig {
  import dev.kamu.core.manifests.parsing.pureconfig.yaml
  import yaml.defaults._
  import pureconfig.generic.auto._

  val configFileName = "transformConfig.yaml"

  def load(): AppConfig = {
    yaml
      .load[Manifest[AppConfig]](getConfigFromResources(configFileName))
      .content
  }

  private def getConfigFromResources(configFileName: String): InputStream = {
    val configStream =
      getClass.getClassLoader.getResourceAsStream(configFileName)

    if (configStream == null)
      throw new RuntimeException(
        s"Unable to locate $configFileName on classpath"
      )

    configStream
  }
}
