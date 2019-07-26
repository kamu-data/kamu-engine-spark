package dev.kamu.core.transform.streaming

import dev.kamu.core.manifests.{
  ProcessingStepSQL,
  TransformStreaming,
  TransformStreamingInput
}
import dev.kamu.core.manifests.utils.fs._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}

class TransformTask(
  val fileSystem: FileSystem,
  val config: AppConfig,
  val spark: SparkSession,
  val transform: TransformStreaming
) {
  val logger = LogManager.getLogger(getClass.getName)

  def setupTransform(): Unit = {
    transform.inputs.foreach(registerInputView)
    transform.steps.foreach(registerStep)
    registerOutput()
  }

  private def registerInputView(input: TransformStreamingInput): Unit = {
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

  private def registerStep(step: ProcessingStepSQL): Unit = {
    logger.info(s"Registering step ${step.view}: ${step.query}")
    spark.sql(step.query).createTempView(s"`${step.view}`")
  }

  private def registerOutput(): Unit = {
    logger.info(s"Registering output: ${transform.id}")

    val outputDir =
      config.repository.dataDirDeriv.resolve(transform.id.toString)
    val checkpointDir =
      config.repository.checkpointDir.resolve(transform.id.toString)

    val outputStream = spark.sql(s"SELECT * FROM `${transform.id}`")

    if (outputStream.isStreaming) {
      logger.info(s"Starting streaming query for: ${transform.id}")

      outputStream.writeStream
        .queryName(getQueryName)
        .trigger(Trigger.ProcessingTime(0))
        .outputMode(OutputMode.Append())
        .partitionBy(transform.partitionBy: _*)
        .option("path", outputDir.toString)
        .option("checkpointLocation", checkpointDir.toString)
        .format("parquet")
        .start()
    } else {
      logger.info(s"Running batch processing for: ${transform.id}")

      outputStream.write
        .mode(SaveMode.Append)
        .partitionBy(transform.partitionBy: _*)
        .parquet(outputDir.toString)
    }
  }

  private def getInputPath(input: TransformStreamingInput): Path = {
    // TODO: Account for dependency graph between datasets
    val derivativePath =
      config.repository.dataDirDeriv.resolve(input.id.toString)
    if (fileSystem.exists(derivativePath))
      derivativePath
    else
      config.repository.dataDirRoot.resolve(input.id.toString)
  }

  private def getQueryName: String = {
    s"{${transform.inputs.mkString(", ")}} -> ${transform.id}"
  }

  private def getInputSchema(input: TransformStreamingInput): StructType = {
    val ds = spark.read.parquet(getInputPath(input).toString)
    ds.schema
  }
}
