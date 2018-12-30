import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

class Transform(config: AppConfig) {
  val logger = LogManager.getLogger(getClass.getName)

  def transform(): Unit = {
    logger.info("Starting transform.streaming")
    logger.info(s"Running with config: $config")

    val spark = SparkSession.builder
      .appName("transform.streaming")
      .getOrCreate()

    val tasks = config.transforms.map(tr => {
      val task = new TransformTask(config, spark.newSession(), tr)
      task.setupTransform()
      task
    })

    logger.info("Stream processing is running")
    spark.streams.awaitAnyTermination()

    logger.info("Terminating")
    tasks.foreach(task => { task.spark.close() })
    spark.close()
  }
}
