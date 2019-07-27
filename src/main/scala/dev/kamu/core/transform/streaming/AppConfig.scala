package dev.kamu.core.transform.streaming

import java.io.InputStream

import dev.kamu.core.manifests.{Dataset, Manifest, RepositoryVolumeMap}

case class AppConfig(
  repository: RepositoryVolumeMap,
  datasets: List[Dataset]
)

object AppConfig {
  import dev.kamu.core.manifests.parsing.pureconfig.yaml
  import yaml.defaults._
  import pureconfig.generic.auto._

  val repositoryConfigFile = "repositoryVolumeMap.yaml"

  def load(): AppConfig = {
    val datasets = findSources()

    val repository = yaml
      .load[Manifest[RepositoryVolumeMap]](
        getConfigFromResources(repositoryConfigFile)
      )
      .content

    val appConfig = AppConfig(
      repository = repository,
      datasets = datasets
    )

    appConfig
  }

  // TODO: This sucks, but searching resources via pattern in Java is a pain
  private def findSources(
    index: Int = 0,
    tail: List[Dataset] = List.empty
  ): List[Dataset] = {
    val stream = getClass.getClassLoader.getResourceAsStream(
      s"dataset_$index.yaml"
    )

    if (stream == null) {
      tail.reverse
    } else {
      val ds = yaml.load[Manifest[Dataset]](stream).content
      if (ds.derivativeSource.isEmpty)
        throw new RuntimeException(
          s"Expected a derivative datasets, got ${ds.kind}"
        )
      findSources(index + 1, ds :: tail)
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
