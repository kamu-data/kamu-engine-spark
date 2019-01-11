import org.apache.log4j.LogManager
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

class Transform(config: AppConfig) {
  val logger = LogManager.getLogger(getClass.getName)

  def transform(): Unit = {
    logger.info("Starting transform.streaming")
    logger.info(s"Running with config: $config")

    val spark = SparkSession.builder
      .appName("transform.streaming")
      // TODO: GeoSpark initialization
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      //
      .getOrCreate()

    val tasks = config.transforms.map(tr => {
      val task = new TransformTask(
        config=config,
        spark=createSparkSubSession(spark),
        transform=tr,
        isStreaming = false)

      task.setupTransform()
      task
    })

    logger.info("Stream processing is running")
    if (spark.streams.active.length != 0)
      spark.streams.awaitAnyTermination()

    logger.info("Terminating")
    tasks.foreach(task => { task.spark.close() })
    spark.close()
  }

  def createSparkSubSession(sparkSession: SparkSession): SparkSession = {
    val subSession = sparkSession.newSession()
    GeoSparkSQLRegistrator.registerAll(subSession)
    subSession
  }
}
