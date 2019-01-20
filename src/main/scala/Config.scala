import java.nio.file.Path


case class InputConfig(
  id: String
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
