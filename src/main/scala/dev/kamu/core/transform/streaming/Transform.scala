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

import dev.kamu.core.manifests._
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

      // TODO: source could've changed several times, so need to respect time
      val source = blocks.reverse
        .find(_.derivativeSource.isDefined)
        .flatMap(_.derivativeSource)
        .get

      val spark = getSparkSubSession(sparkSession)

      val inputIntervals = getInputSlices(taskConfig, source, outputMetaChain)

      if (inputIntervals.values.forall(_.interval.isEmpty)) {
        logger.info(s"No new input data to process - skipping")
      } else {

        // Setup inputs
        val inputSlices = inputIntervals.map({
          case (id, slice) =>
            val newSlice = prepareInputSlice(
              spark,
              id,
              slice.interval,
              taskConfig.datasetLayouts(id.toString)
            )
            (id, newSlice)
        })

        // Setup transform
        for (step <- source.steps) {
          step match {
            case s: ProcessingStepKind.SparkSQL =>
              spark
                .sql(s.query)
                .createTempView(
                  s"`${s.alias.getOrElse(taskConfig.datasetToTransform)}`"
                )
            case _ =>
              throw new RuntimeException(
                s"Unsupported processing step kind: $step"
              )
          }
        }

        // Write output
        val result = spark
          .sql(s"SELECT * FROM `${taskConfig.datasetToTransform}`")

        result.cache()

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

        result.write
          .mode(SaveMode.Append)
          .parquet(outputLayout.dataDir.toString)

        result.unpersist()

        val nextBlock = MetadataBlock(
          prevBlockHash = blocks.last.blockHash,
          // TODO: Current time? Min of input times? Require to propagate in computations?
          systemTime = systemClock.instant(),
          outputSlice = Some(
            DataSlice(
              hash = resultHash,
              interval = resultInterval,
              numRecords = resultNumRecords
            )
          ),
          inputSlices = inputSlices
            .map({ case (id, slice) => (id.toString(), slice) })
        )

        // TODO: Atomicity?
        val newBlock = outputMetaChain.append(nextBlock)
        outputMetaChain.updateSummary(
          s =>
            s.copy(
              lastPulled = Some(systemClock.instant()),
              numRecords = s.numRecords + resultNumRecords,
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

  def getInputSlices(
    taskConfig: TransformTaskConfig,
    source: DerivativeSource,
    outputMetaChain: MetadataChainFS
  ): Map[DatasetID, DataSlice] = {
    source.inputs
      .map(input => {
        val inputMetaChain = new MetadataChainFS(
          fileSystem,
          taskConfig.datasetLayouts(input.id.toString).metadataDir
        )

        (
          input.id,
          DataSlice(
            hash = "",
            interval = getInputSliceInterval(
              input.id,
              inputMetaChain,
              outputMetaChain
            ),
            numRecords = -1
          )
        )
      })
      .toMap
  }

  def getInputSliceInterval(
    inputID: DatasetID,
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
      .flatMap(_.inputSlices.get(inputID.toString))
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

  def prepareInputSlice(
    spark: SparkSession,
    id: DatasetID,
    interval: Interval[Instant],
    layout: DatasetLayout
  ): DataSlice = {
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

    df.createTempView(s"`$id`")

    DataSlice(
      hash = computeHash(df),
      interval = interval,
      numRecords = df.count()
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
