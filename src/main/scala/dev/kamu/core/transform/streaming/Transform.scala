/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.core.transform.streaming

import java.sql.Timestamp
import java.time.Instant

import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.infra.MetadataChainFS
import dev.kamu.core.utils.{DataFrameDigestSHA1, ManualClock}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import spire.math.interval.{Closed, Open, Unbound, ValueBound}
import spire.math.{Empty, Interval}

case class InputSlice(
  dataFrame: DataFrame,
  dataSlice: DataSlice
)

class Transform(config: AppConfig) {
  val logger = LogManager.getLogger(getClass.getName)
  val systemClock = new ManualClock()
  val fileSystem = FileSystem.get(hadoopConf)

  def transform(): Unit = {
    logger.info("Starting transform.streaming")
    logger.info(s"Running with config: $config")

    // TODO: Currently performing operation in iterative batch fashion for simplicity
    // Should be converted back to stream processing
    for (taskConfig <- config.tasks) {
      logger.info(s"Processing dataset: ${taskConfig.datasetToTransform}")
      systemClock.advance()

      val outputLayout =
        taskConfig.datasetLayouts(taskConfig.datasetToTransform.toString)

      val outputMetaChain = new MetadataChainFS(
        fileSystem,
        outputLayout.metadataDir
      )

      val blocks = outputMetaChain.getBlocks()

      val (source, transform) = getSourceAndTransform(outputMetaChain)

      val inputIntervals =
        getInputIntervals(taskConfig, source, outputMetaChain)

      if (inputIntervals.values.forall(_.isEmpty)) {
        logger.info(s"No new input data to process - skipping")
      } else {
        val nextBlock = transformBatch(
          taskConfig.datasetToTransform,
          inputIntervals,
          taskConfig.datasetLayouts.map {
            case (id, layout) => (DatasetID(id), layout)
          },
          source,
          transform
        )

        // TODO: Atomicity?
        val newBlock = outputMetaChain.append(
          nextBlock.copy(prevBlockHash = blocks.last.blockHash)
        )

        outputMetaChain.updateSummary(
          s =>
            s.copy(
              lastPulled = Some(systemClock.instant()),
              numRecords = s.numRecords + newBlock.outputSlice.get.numRecords,
              dataSize = fileSystem
                .getContentSummary(outputLayout.dataDir)
                .getSpaceConsumed
            )
        )

        logger.info(
          s"Done processing dataset: ${taskConfig.datasetToTransform} (${newBlock.blockHash})"
        )
      }
    }

    logger.info("Finished")
  }

  def transformBatch(
    datasetID: DatasetID,
    inputIntervals: Map[DatasetID, Interval[Instant]],
    datasetLayouts: Map[DatasetID, DatasetLayout],
    source: DerivativeSource,
    transform: TransformKind.SparkSQL
  ): MetadataBlock = {
    val spark = getSparkSubSession(sparkSession)

    // Setup inputs
    val inputs = prepareInputSlices(spark, inputIntervals, datasetLayouts)

    // Setup transform
    for (step <- transform.queries) {
      spark
        .sql(step.query)
        .createTempView(s"`${step.alias.getOrElse(datasetID)}`")
    }

    // Process data
    val result = spark
      .sql(s"SELECT * FROM `$datasetID`")

    result.cache()

    // Compute metadata
    val (resultHash, resultInterval, resultNumRecords) =
      if (!result.isEmpty) {
        (
          computeHash(result),
          Interval.point(systemClock.instant()),
          result.count()
        )
      } else {
        ("", Interval.empty[Instant], 0L)
      }

    // Write data
    result.write
      .mode(SaveMode.Append)
      .parquet(datasetLayouts(datasetID).dataDir.toString)

    result.unpersist(true)
    inputs.values.foreach(_.dataFrame.unpersist(true))

    MetadataBlock(
      prevBlockHash = "",
      // TODO: Current time? Min of input times? Require to propagate in computations?
      systemTime = systemClock.instant(),
      outputSlice = Some(
        DataSlice(
          hash = resultHash,
          interval = resultInterval,
          numRecords = resultNumRecords
        )
      ),
      inputSlices = source.inputs.map(i => inputs(i.id).dataSlice)
    )
  }

  def getSourceAndTransform(
    outputMetaChain: MetadataChainFS
  ): (DerivativeSource, TransformKind.SparkSQL) = {
    val sources = outputMetaChain
      .getBlocks()
      .reverse
      .flatMap(_.derivativeSource)

    // TODO: source could've changed several times
    if (sources.length > 1)
      throw new RuntimeException("Transform evolution is not yet supported")

    val source = sources.head

    if (source.transformEngine != "sparkSQL")
      throw new RuntimeException(
        s"Unsupported engine: ${source.transformEngine}"
      )

    val transform = yaml.load[TransformKind.SparkSQL](source.transform.toConfig)

    (source, transform)
  }

  def getInputIntervals(
    taskConfig: TransformTaskConfig,
    source: DerivativeSource,
    outputMetaChain: MetadataChainFS
  ): Map[DatasetID, Interval[Instant]] = {
    source.inputs.zipWithIndex.map {
      case (input, index) =>
        val inputMetaChain = new MetadataChainFS(
          fileSystem,
          taskConfig.datasetLayouts(input.id.toString).metadataDir
        )

        (
          input.id,
          getInputSliceInterval(
            input.id,
            index,
            inputMetaChain,
            outputMetaChain
          )
        )
    }.toMap
  }

  def getInputSliceInterval(
    inputID: DatasetID,
    inputIndex: Int,
    inputMetaChain: MetadataChainFS,
    outputMetaChain: MetadataChainFS
  ): Interval[Instant] = {

    // Determine available data range
    // Result is either: () or (-inf, upper]
    val ivAvailable = inputMetaChain
      .getBlocks()
      .reverse
      .flatMap(_.outputSlice)
      .find(_.interval.nonEmpty)
      .map(_.interval)
      .map(i => Interval.fromBounds(Unbound(), i.upperBound))
      .getOrElse(Interval.empty)

    // Determine processed data range
    // Result is either: () or (inf, upper] or (lower, upper]
    val ivProcessed = outputMetaChain
      .getBlocks()
      .reverse
      .filter(_.inputSlices.nonEmpty)
      .map(_.inputSlices(inputIndex))
      .find(_.interval.nonEmpty)
      .map(_.interval)
      .getOrElse(Interval.empty)

    // Determine unprocessed data range
    // Result is either: (-inf, inf) or (lower, inf)
    val ivUnprocessed = ivProcessed.upperBound match {
      case ValueBound(upper) =>
        Interval.above(upper)
      case _ =>
        Interval.all
    }

    // Result is either: () or (lower, upper]
    val ivToProcess = ivAvailable & ivUnprocessed

    logger.info(
      s"Input range for $inputID is: $ivToProcess (available: $ivAvailable, processed: $ivProcessed)"
    )
    ivToProcess
  }

  def prepareInputSlices(
    spark: SparkSession,
    inputIntervals: Map[DatasetID, Interval[Instant]],
    inputLayouts: Map[DatasetID, DatasetLayout]
  ): Map[DatasetID, InputSlice] = {
    inputIntervals.map({
      case (id, interval) =>
        val inputSlice = prepareInputSlice(
          spark,
          id,
          interval,
          inputLayouts(id)
        )
        (id, inputSlice)
    })
  }

  def prepareInputSlice(
    spark: SparkSession,
    id: DatasetID,
    interval: Interval[Instant],
    layout: DatasetLayout
  ): InputSlice = {
    val inputMetaChain = new MetadataChainFS(fileSystem, layout.metadataDir)
    val vocab = inputMetaChain
      .getSummary()
      .vocabulary
      .getOrElse(DatasetVocabularyOverrides())
      .asDatasetVocabulary()

    // TODO: use schema from metadata
    val df = spark.read
      .parquet(layout.dataDir.toString)
      .transform(sliceData(interval, vocab))

    df.cache()
    df.createTempView(s"`$id`")

    InputSlice(
      dataFrame = df,
      dataSlice = DataSlice(
        hash = computeHash(df),
        interval = interval,
        numRecords = df.count()
      )
    )
  }

  def sliceData(interval: Interval[Instant], vocab: DatasetVocabulary)(
    df: DataFrame
  ): DataFrame = {
    interval match {
      case Empty() =>
        df.where(lit(false))
      case _ =>
        val col = df.col(vocab.systemTimeColumn)

        val dfLower = interval.lowerBound match {
          case Unbound() =>
            df
          case Open(x) =>
            df.filter(col > Timestamp.from(x))
          case Closed(x) =>
            df.filter(col >= Timestamp.from(x))
          case _ =>
            throw new RuntimeException(s"Unexpected: $interval")
        }

        interval.upperBound match {
          case Unbound() =>
            dfLower
          case Open(x) =>
            dfLower.filter(col < Timestamp.from(x))
          case Closed(x) =>
            dfLower.filter(col <= Timestamp.from(x))
          case _ =>
            throw new RuntimeException(s"Unexpected: $interval")
        }
    }
  }

  def computeHash(df: DataFrame): String = {
    // TODO: drop system time column first?
    new DataFrameDigestSHA1().digest(df)
  }

  def sparkConf: SparkConf = {
    new SparkConf()
      .setAppName("transform.streaming")
      .set("spark.sql.session.timeZone", "UTC")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
  }

  def hadoopConf: org.apache.hadoop.conf.Configuration = {
    SparkHadoopUtil.get.newConfiguration(sparkConf)
  }

  def sparkSession: SparkSession = {
    SparkSession.builder
      .config(sparkConf)
      .getOrCreate()
  }

  def getSparkSubSession(sparkSession: SparkSession): SparkSession = {
    val subSession = sparkSession.newSession()
    GeoSparkSQLRegistrator.registerAll(subSession)
    subSession
  }
}
