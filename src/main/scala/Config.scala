import java.nio.file.Path


case class Input(
  id: String,
  schemaName: String,
  kind: String
)


case class Output(
  id: String
)


case class AppConfig(
  dataRootDir: Path,
  dataDerivativeDir: Path,
  checkpointDir: Path,
  input: Input,
  output: Output
)
