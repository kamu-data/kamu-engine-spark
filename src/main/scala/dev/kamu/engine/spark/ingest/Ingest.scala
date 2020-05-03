/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest

import java.sql.Timestamp
import java.time.Instant
import java.util.zip.ZipInputStream

import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import pureconfig.generic.auto._
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.{Clock, DataFrameDigestSHA256, ZipFiles}
import dev.kamu.engine.spark.ingest.merge.MergeStrategy
import dev.kamu.engine.spark.ingest.utils.DFUtils._
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.datasyslab.geospark.formatMapper.GeoJsonReader
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geosparksql.utils.Adapter
import spire.math.Interval

class Ingest(
  fileSystem: FileSystem,
  systemClock: Clock
) {
  private val logger = LogManager.getLogger(getClass.getName)

  def ingest(spark: SparkSession, task: IngestTask): Unit = {
    val block = ingest(
      spark,
      task.source,
      task.eventTime,
      task.dataToIngest,
      task.datasetLayout.dataDir,
      task.datasetVocab
    )

    val ouptutStream =
      fileSystem.create(task.metadataOutputDir.resolve("block.yaml"))
    yaml.save(Manifest(block), ouptutStream)
    ouptutStream.close()
  }

  private def ingest(
    spark: SparkSession,
    source: SourceKind.Root,
    eventTime: Option[Instant],
    filePath: Path,
    outPath: Path,
    vocab: DatasetVocabulary
  ): MetadataBlock = {
    logger.info(
      s"Ingesting the data: in=$filePath, out=$outPath, format=${source.read}"
    )

    // Needed to make Spark re-read files that might've changed between ingest runs
    spark.sqlContext.clearCache()

    val reader = source.read match {
      case _: ReaderKind.Shapefile =>
        readShapefile _
      case _: ReaderKind.Geojson =>
        readGeoJSON _
      case _ =>
        readGeneric _
    }

    val result = reader(spark, source, filePath, vocab)
      .transform(checkForErrors(vocab))
      .transform(normalizeSchema(source))
      .transform(preprocess(source))
      .transform(mergeWithExisting(source, eventTime, outPath, vocab))
      .maybeTransform(source.coalesce != 0, _.coalesce(source.coalesce))

    result.cache()

    val hash = computeHash(result)
    val numRecords = result.count()

    result.write
      .mode(SaveMode.Append)
      .parquet(outPath.toString)

    result.unpersist()

    MetadataBlock(
      prevBlockHash = "",
      systemTime = systemClock.instant(),
      outputSlice = Some(
        DataSlice(
          hash = hash,
          numRecords = numRecords,
          interval = Interval.point(systemClock.instant())
        )
      )
    )
  }

  private def readGeneric(
    spark: SparkSession,
    source: SourceKind.Root,
    filePath: Path,
    vocab: DatasetVocabulary
  ): DataFrame = {
    val fmt = source.read.asGeneric().asInstanceOf[ReaderKind.Generic]
    val reader = spark.read

    if (fmt.schema.nonEmpty)
      reader.schema(fmt.schema.mkString(", "))

    reader
      .format(fmt.name)
      .options(fmt.options)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", vocab.corruptRecordColumn)
      .load(filePath.toString)
  }

  // TODO: This is inefficient
  private def readShapefile(
    spark: SparkSession,
    source: SourceKind.Root,
    filePath: Path,
    vocab: DatasetVocabulary
  ): DataFrame = {
    val fmt = source.read.asInstanceOf[ReaderKind.Shapefile]

    val extractedPath = filePath.getParent.resolve("shapefile")

    val inputStream = fileSystem.open(filePath)
    val bzip2Stream = new BZip2CompressorInputStream(inputStream)
    val zipStream = new ZipInputStream(bzip2Stream)

    ZipFiles.extractZipFile(
      fileSystem,
      zipStream,
      extractedPath,
      fmt.subPathRegex
    )

    zipStream.close()

    val rdd = ShapefileReader.readToGeometryRDD(
      spark.sparkContext,
      extractedPath.toString
    )

    Adapter
      .toDf(rdd, spark)
      .withColumn(
        "geometry",
        functions.callUDF("ST_GeomFromWKT", functions.col("geometry"))
      )
  }

  // TODO: This is very inefficient, should extend GeoSpark to support this
  private def readGeoJSON(
    spark: SparkSession,
    source: SourceKind.Root,
    filePath: Path,
    vocab: DatasetVocabulary
  ): DataFrame = {
    val rdd = GeoJsonReader.readToGeometryRDD(
      spark.sparkContext,
      filePath.toString,
      false,
      false
    )

    Adapter
      .toDf(rdd, spark)
      .withColumn(
        "geometry",
        functions.callUDF("ST_GeomFromWKT", functions.col("geometry"))
      )
  }

  private def checkForErrors(
    vocab: DatasetVocabulary
  )(df: DataFrame): DataFrame = {
    df.getColumn(vocab.corruptRecordColumn) match {
      case None =>
        df
      case Some(col) =>
        val dfCached = df.cache()
        val corrupt = dfCached.select(col).filter(col.isNotNull)
        if (corrupt.count() > 0) {
          throw new Exception(
            "Corrupt records detected:\n" + corrupt
              .showString(numRows = 20, truncate = 0)
          )
        } else {
          dfCached.drop(col)
        }
    }
  }

  private def normalizeSchema(
    source: SourceKind.Root
  )(df: DataFrame): DataFrame = {
    if (source.read.schema.nonEmpty)
      return df

    var result = df
    for (col <- df.columns) {
      result = result.withColumnRenamed(
        col,
        col
          .replaceAll("[ ,;{}()=]", "_")
          .replaceAll("[\\n\\r\\t]", "")
      )
    }
    result
  }

  private def preprocess(source: SourceKind.Root)(df: DataFrame): DataFrame = {
    if (source.preprocess.isEmpty)
      return df

    if (source.preprocessEngine.get != "sparkSQL")
      throw new RuntimeException(
        s"Unsupported engine: ${source.preprocessEngine.get}"
      )

    val transform =
      yaml.load[TransformKind.SparkSQL](source.preprocess.get.toConfig)

    val spark = df.sparkSession
    df.createTempView("input")

    for (step <- transform.queries) {
      val tempResult = spark.sql(step.query)
      if (step.alias.isEmpty || step.alias.get == "output")
        return tempResult
      else
        tempResult.createTempView(s"`${step.alias.get}`")
    }

    throw new RuntimeException(
      "Pre-processing steps do not contain output query"
    )
  }

  private def mergeWithExisting(
    source: SourceKind.Root,
    eventTime: Option[Instant],
    outPath: Path,
    vocab: DatasetVocabulary
  )(
    curr: DataFrame
  ): DataFrame = {
    val spark = curr.sparkSession
    val mergeStrategy = MergeStrategy(
      kind = source.merge,
      systemClock = systemClock,
      eventTime = eventTime.map(Timestamp.from),
      vocab = vocab
    )

    val prev =
      if (fileSystem.exists(outPath))
        Some(spark.read.parquet(outPath.toString))
      else
        None

    // TODO: Cache prev and curr?
    mergeStrategy.merge(prev, curr)
  }

  private def computeHash(df: DataFrame): String = {
    // TODO: drop system time column first?
    new DataFrameDigestSHA256().digest(df)
  }

}
