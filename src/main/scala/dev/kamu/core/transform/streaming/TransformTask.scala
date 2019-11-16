package dev.kamu.core.transform.streaming

import dev.kamu.core.manifests.{DerivativeInput, ProcessingStepSQL}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}

class TransformTask(
  val fileSystem: FileSystem,
  val config: AppConfig,
  val spark: SparkSession,
  val taskConfig: TransformTaskConfig
) {
  val logger = LogManager.getLogger(getClass.getName)

  val transform = taskConfig.datasetToTransform.derivativeSource.get

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
    val dataset = taskConfig.datasetToTransform

    logger.info(s"Registering output: ${dataset.id}")

    val outputStream = spark.sql(s"SELECT * FROM `${dataset.id}`")

    if (outputStream.isStreaming) {
      logger.info(s"Starting streaming query for: ${dataset.id}")

      outputStream.writeStream
        .queryName(getQueryName)
        .trigger(Trigger.ProcessingTime(0))
        .outputMode(OutputMode.Append())
        .partitionBy(transform.partitionBy: _*)
        .option("path", taskConfig.outputDataPath.toString)
        .option("checkpointLocation", taskConfig.checkpointsPath.toString)
        .format("parquet")
        .start()
    } else {
      logger.info(s"Running batch processing for: ${dataset.id}")

      outputStream.write
        .mode(SaveMode.Append)
        .partitionBy(transform.partitionBy: _*)
        .parquet(taskConfig.outputDataPath.toString)
    }
  }

  private def getInputPath(input: DerivativeInput): Path = {
    taskConfig.inputDataPaths(input.id.toString)
  }

  private def getQueryName: String = {
    s"{${transform.inputs.mkString(", ")}} -> ${taskConfig.datasetToTransform.id}"
  }

  private def getInputSchema(input: DerivativeInput): StructType = {
    val ds = spark.read.parquet(getInputPath(input).toString)
    ds.schema
  }
}
