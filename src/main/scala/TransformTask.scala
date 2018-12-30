import java.nio.file.Path

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

class TransformTask(
  val config: AppConfig,
  val spark: SparkSession,
  val transform: TransformConfig
) {
  val logger = LogManager.getLogger(getClass.getName)

  def setupTransform(): Unit = {
    transform.inputs.foreach(registerInputView)
    transform.steps.foreach(registerStep)
    transform.outputs.foreach(registerOutput)
  }

  private def getQueryName(transform: TransformConfig): String = {
    s"${transform.inputs(0).id} -> ${transform.outputs(0).id}"
  }

  private def getInputPath(input: InputConfig): Path = {
    input.kind match {
      case "root" =>
        config.dataRootDir.resolve(input.id)
      case "derivative" =>
        config.dataDerivativeDir.resolve(input.id)
    }
  }

  private def registerInputView(input: InputConfig): Unit = {
    logger.info(s"Registering input: ${input.id}")

    val inputStream = spark.readStream
      .schema(Schemas.schemas(input.schemaName))
      .parquet(getInputPath(input).toString)

    inputStream.createTempView(s"`${input.id}`")
  }

  private def registerStep(sqlString: String): Unit ={
    logger.info(s"Registering step: $sqlString")

    spark.sql(sqlString)
  }

  private def registerOutput(output: OutputConfig): Unit = {
    logger.info(s"Registering output: ${output.id}")

    val outputStream = spark.sql(s"SELECT * FROM `${output.id}`")

    outputStream.writeStream
      .queryName(getQueryName(transform))
      .trigger(Trigger.ProcessingTime(0))
      .outputMode(OutputMode.Append())
      .option("path", config.dataDerivativeDir.resolve(output.id).toString)
      .option("checkpointLocation", config.checkpointDir.resolve(output.id).toString)
      .format("parquet")
      .start()
  }
}
