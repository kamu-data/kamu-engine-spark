import java.net.URI

import org.apache.hadoop.fs.Path
import pureconfig.generic.ProductHint
import pureconfig.module.yaml.loadYamlOrThrow
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigReader}
import pureconfig.generic.auto._


case class InputConfig(
  id: String,

  /*** Defines the mode in which this input should be open
    *
    * Valid values are:
    *  - batch
    *  - stream
    */
  mode: String = "stream"
)


case class OutputConfig(
  id: String,
  partitionBy: Vector[String] = Vector.empty
)


case class TransformConfig(
  id: String,
  inputs: Vector[InputConfig],
  outputs: Vector[OutputConfig],
  steps: Vector[StepConf]
)


case class StepConf(
  view: String,
  query: String
)


case class AppConfig(
  dataRootDir: Path,
  dataDerivativeDir: Path,
  checkpointDir: Path,
  transforms: Vector[TransformConfig]
)


object AppConfig {
  val configFileName = "transform-config.yaml"

  // Reader for hadoop fs paths
  implicit val pathReader = ConfigReader[String]
    .map(s => new Path(URI.create(s)))

  // Hint to use camel case for config field names
  implicit def hint[T]: ProductHint[T] =
    ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  def load(): AppConfig = {
    val configStream = getClass.getClassLoader.getResourceAsStream(configFileName)
    if (configStream == null)
      throw new RuntimeException(
        s"Unable to locate $configFileName on classpath")

    val configString = scala.io.Source.fromInputStream(configStream).mkString
    val config = loadYamlOrThrow[AppConfig](configString)

    config
  }
}
