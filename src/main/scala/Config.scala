import java.nio.file.Path


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
  steps: Vector[String]
)


case class AppConfig(
  dataRootDir: Path,
  dataDerivativeDir: Path,
  checkpointDir: Path,
  transforms: Vector[TransformConfig]
)
