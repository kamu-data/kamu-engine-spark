package dev.kamu.core.transform.streaming

import java.io.InputStream

import dev.kamu.core.manifests.{Dataset, Manifest, Resource}
import org.apache.hadoop.fs.Path

case class TransformTaskConfig(
  datasetToTransform: Dataset,
  inputDataPaths: Map[String, Path],
  checkpointsPath: Path,
  outputDataPath: Path
) extends Resource[TransformTaskConfig]

case class AppConfig(
  tasks: Vector[TransformTaskConfig]
) extends Resource[AppConfig]

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
