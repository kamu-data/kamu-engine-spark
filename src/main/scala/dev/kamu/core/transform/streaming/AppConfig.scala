package dev.kamu.core.transform.streaming

import java.io.InputStream

import dev.kamu.core.manifests.{
  Manifest,
  RepositoryVolumeMap,
  TransformStreaming
}

case class AppConfig(
  repository: RepositoryVolumeMap,
  transforms: List[TransformStreaming]
)

object AppConfig {
  import dev.kamu.core.manifests.parsing.pureconfig.yaml
  import yaml.defaults._
  import pureconfig.generic.auto._

  val repositoryConfigFile = "repositoryVolumeMap.yaml"

  def load(): AppConfig = {
    val transforms = findSources()

    val repository = yaml
      .load[Manifest[RepositoryVolumeMap]](
        getConfigFromResources(repositoryConfigFile)
      )
      .content

    val appConfig = AppConfig(
      repository = repository,
      transforms = transforms
    )

    appConfig
  }

  // TODO: This sucks, but searching resources via pattern in Java is a pain
  private def findSources(
    index: Int = 0,
    tail: List[TransformStreaming] = List.empty
  ): List[TransformStreaming] = {
    val stream = getClass.getClassLoader.getResourceAsStream(
      s"transformStreaming_$index.yaml"
    )

    if (stream == null) {
      tail.reverse
    } else {
      val source = yaml.load[Manifest[TransformStreaming]](stream).content
      findSources(index + 1, source :: tail)
    }
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
