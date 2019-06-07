package dev.kamu.core.transform.streaming

import dev.kamu.core.transform.streaming.FSUtils._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}

class TransformTask(
  val fileSystem: FileSystem,
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

  private def registerInputView(input: InputConfig): Unit = {
    logger.info(s"Registering input: ${input.id}")

    val inputStream = input.mode.toLowerCase match {
      case "stream" =>
        spark.readStream
          .schema(getInputSchema(input))
          .parquet(getInputPath(input).toString)
      case "batch" =>
        spark.read
          .schema(getInputSchema(input))
          .parquet(getInputPath(input).toString)
    }

    inputStream.createTempView(s"`${input.id}`")
  }

  private def registerStep(step: StepConf): Unit = {
    logger.info(s"Registering step ${step.view}: ${step.query}")
    spark.sql(step.query).createTempView(s"`${step.view}`")
  }

  private def registerOutput(output: OutputConfig): Unit = {
    logger.info(s"Registering output: ${output.id}")

    val outputDir = config.dataDerivativeDir.resolve(output.id)
    val checkpointDir = config.checkpointDir.resolve(output.id)

    val outputStream = spark.sql(s"SELECT * FROM `${output.id}`")

    if (outputStream.isStreaming) {
      logger.info(s"Starting streaming query for: ${output.id}")

      outputStream.writeStream
        .queryName(getQueryName(output))
        .trigger(Trigger.ProcessingTime(0))
        .outputMode(OutputMode.Append())
        .partitionBy(output.partitionBy: _*)
        .option("path", outputDir.toString)
        .option("checkpointLocation", checkpointDir.toString)
        .format("parquet")
        .start()
    } else {
      logger.info(s"Running batch processing for: ${output.id}")

      outputStream.write
        .mode(SaveMode.Append)
        .partitionBy(output.partitionBy: _*)
        .parquet(outputDir.toString)
    }
  }

  private def getInputPath(input: InputConfig): Path = {
    // TODO: Account for dependency graph between datasets
    val derivativePath = config.dataDerivativeDir.resolve(input.id)
    if (fileSystem.exists(derivativePath))
      derivativePath
    else
      config.dataRootDir.resolve(input.id)
  }

  private def getQueryName(output: OutputConfig): String = {
    s"{${transform.inputs.mkString(", ")}} -> ${output.id}"
  }

  private def getInputSchema(input: InputConfig): StructType = {
    val ds = spark.read.parquet(getInputPath(input).toString)
    ds.schema
  }
}
