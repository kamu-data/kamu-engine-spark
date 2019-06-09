package dev.kamu.core.transform.streaming

import java.io.InputStream

import dev.kamu.core.manifests.{
  Manifest,
  RepositoryVolumeMap,
  TransformStreaming
}

case class AppConfig(
  repository: RepositoryVolumeMap,
  transforms: Vector[TransformStreaming]
)

object AppConfig {
  import dev.kamu.core.manifests.parsing.pureconfig.yaml
  import yaml.defaults._
  import pureconfig.generic.auto._

  val repositoryConfigFile = "repositoryVolumeMap.yaml"
  val transformStreamingConfigFile = "transformStreaming.yaml"

  def load(): AppConfig = {
    val transform = yaml
      .load[Manifest[TransformStreaming]](
        getConfigFromResources(transformStreamingConfigFile)
      )
      .content

    val repository = yaml
      .load[Manifest[RepositoryVolumeMap]](
        getConfigFromResources(repositoryConfigFile)
      )
      .content

    val appConfig = AppConfig(
      repository = repository,
      transforms = Vector(transform)
    )

    appConfig
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
