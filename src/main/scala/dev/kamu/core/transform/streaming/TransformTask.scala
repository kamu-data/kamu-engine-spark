package dev.kamu.core.transform.streaming

import dev.kamu.core.manifests.{
  Dataset,
  DerivativeSource,
  DerivativeInput,
  ProcessingStepSQL
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
  val dataset: Dataset
) {
  val logger = LogManager.getLogger(getClass.getName)

  val transform = dataset.derivativeSource.get

  def setupTransform(): Unit = {
    transform.inputs.foreach(registerInputView)
    transform.steps.foreach(registerStep)
    registerOutput()
  }

  private def registerInputView(input: DerivativeInput): Unit = {
    logger.info(s"Registering input: ${input.id}")

    val inputStream = input.mode match {
      case DerivativeInput.Mode.Stream =>
        spark.readStream
          .schema(getInputSchema(input))
          .parquet(getInputPath(input).toString)
      case DerivativeInput.Mode.Batch =>
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
    logger.info(s"Registering output: ${dataset.id}")

    val outputDir =
      config.repository.dataDirDeriv.resolve(dataset.id.toString)
    val checkpointDir =
      config.repository.checkpointDir.resolve(dataset.id.toString)

    val outputStream = spark.sql(s"SELECT * FROM `${dataset.id}`")

    if (outputStream.isStreaming) {
      logger.info(s"Starting streaming query for: ${dataset.id}")

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
      logger.info(s"Running batch processing for: ${dataset.id}")

      outputStream.write
        .mode(SaveMode.Append)
        .partitionBy(transform.partitionBy: _*)
        .parquet(outputDir.toString)
    }
  }

  private def getInputPath(input: DerivativeInput): Path = {
    // TODO: Account for dependency graph between datasets
    val derivativePath =
      config.repository.dataDirDeriv.resolve(input.id.toString)
    if (fileSystem.exists(derivativePath))
      derivativePath
    else
      config.repository.dataDirRoot.resolve(input.id.toString)
  }

  private def getQueryName: String = {
    s"{${transform.inputs.mkString(", ")}} -> ${dataset.id}"
  }

  private def getInputSchema(input: DerivativeInput): StructType = {
    val ds = spark.read.parquet(getInputPath(input).toString)
    ds.schema
  }
}
