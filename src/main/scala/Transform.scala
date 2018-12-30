import java.nio.file.Path

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

class Transform(config: AppConfig) {
  val logger = LogManager.getLogger(getClass.getName)

  def transform(): Unit = {
    logger.info("Starting transform")
    logger.info(s"Running with config: $config")

    val spark = SparkSession.builder
      .appName(config.output.id)
      .getOrCreate()

    val stream = spark.readStream
      .schema(Schemas.schemas(config.input.schemaName))
      .parquet(getInputPath(config.input).toString)

    stream.writeStream
      .queryName(config.output.id)
      .trigger(Trigger.ProcessingTime(0))
      .outputMode(OutputMode.Append())
      .format("parquet")
      .option("checkpointLocation", config.checkpointDir.resolve(config.output.id).toString)
      .option("path", config.dataDerivativeDir.resolve(config.output.id).toString)
      .start()

    spark.streams.awaitAnyTermination()
    spark.close()
  }

  private def getInputPath(input: Input): Path = {
    input.kind match {
      case "root" =>
        config.dataRootDir.resolve(input.id)
      case "derivative" =>
        config.dataDerivativeDir.resolve(input.id)
    }
  }
}
