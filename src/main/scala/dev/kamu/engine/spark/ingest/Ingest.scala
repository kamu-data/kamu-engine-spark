/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest

import java.io.PrintWriter
import java.sql.Timestamp
import java.time.Instant
import java.util.Scanner
import java.util.zip.ZipInputStream

import dev.kamu.core.manifests._
import dev.kamu.core.manifests.infra.{IngestRequest, IngestResult}
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
import org.apache.spark.sql.functions.lit
import org.datasyslab.geospark.formatMapper.GeoJsonReader
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geosparksql.utils.Adapter
import spire.math.Interval

class Ingest(
  fileSystem: FileSystem,
  systemClock: Clock
) {
  private val corruptRecordColumn = "__corrupt_record__"
  private val logger = LogManager.getLogger(getClass.getName)

  def ingest(spark: SparkSession, request: IngestRequest): IngestResult = {
    val block = ingest(
      spark,
      request.source,
      request.eventTime,
      request.dataToIngest,
      request.datasetLayout.checkpointsDir,
      request.datasetLayout.dataDir,
      request.datasetVocab.withDefaults()
    )

    IngestResult(block = block)
  }

  private def ingest(
    spark: SparkSession,
    source: SourceKind.Root,
    eventTime: Option[Instant],
    filePath: Path,
    checkpointsDir: Path,
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

    val result = reader(spark, source, filePath)
      .transform(checkForErrors)
      .transform(normalizeSchema(source))
      .transform(preprocess(source))
      .transform(mergeWithExisting(source, eventTime, outPath, vocab))
      .transform(postprocess(vocab))

    result.cache()

    val block = MetadataBlock(
      prevBlockHash = "",
      systemTime = systemClock.instant(),
      outputSlice = Some(
        DataSlice(
          hash = computeHash(result.drop(vocab.systemTimeColumn.get)),
          numRecords = result.count(),
          interval =
            if (result.isEmpty) Interval.empty
            else Interval.point(systemClock.instant())
        )
      ),
      outputWatermark = getLastWatermark(result, checkpointsDir, vocab)
    )

    writeParquet(result, outPath)

    result.unpersist()

    block
  }

  private def readGeneric(
    spark: SparkSession,
    source: SourceKind.Root,
    filePath: Path
  ): DataFrame = {
    val fmt = source.read.asGeneric().asInstanceOf[ReaderKind.Generic]
    val reader = spark.read

    if (fmt.schema.nonEmpty)
      reader.schema(fmt.schema.mkString(", "))

    reader
      .format(fmt.name)
      .options(fmt.options)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", corruptRecordColumn)
      .load(filePath.toString)
  }

  // TODO: This is inefficient
  private def readShapefile(
    spark: SparkSession,
    source: SourceKind.Root,
    filePath: Path
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
    filePath: Path
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

  private def checkForErrors(df: DataFrame): DataFrame = {
    df.getColumn(corruptRecordColumn) match {
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
      eventTimeColumn = vocab.eventTimeColumn.get,
      eventTime = Timestamp.from(eventTime.getOrElse(systemClock.instant()))
    )

    // Drop system columns before merging
    val prev = Some(outPath)
      .filter(fileSystem.exists)
      .map(
        p =>
          spark.read
            .parquet(p.toString)
            .drop(vocab.systemTimeColumn.get)
      )

    // TODO: Cache prev and curr?
    mergeStrategy.merge(prev, curr)
  }

  private def postprocess(
    vocab: DatasetVocabulary
  )(df: DataFrame): DataFrame = {
    if (df.getColumn(vocab.systemTimeColumn.get).isDefined)
      throw new Exception(
        s"Ingested data contains a column that conflicts with the system column name, " +
          s"you should either rename the data column or configure the dataset vocabulary " +
          s"to use a different name: ${vocab.systemTimeColumn.get}"
      )

    df.coalesce(1)
      .orderBy(vocab.eventTimeColumn.get)
      .withColumn(vocab.systemTimeColumn.get, lit(systemClock.timestamp()))
      .columnToFront(vocab.systemTimeColumn.get)
  }

  private def writeParquet(df: DataFrame, outDir: Path): Path = {
    val tmpOutDir = outDir.resolve(".tmp")

    df.write.parquet(tmpOutDir.toString)

    val dataFiles = fileSystem
      .listStatus(tmpOutDir)
      .filter(_.getPath.getName.endsWith(".snappy.parquet"))

    if (dataFiles.length != 1)
      throw new RuntimeException(
        "Unexpected number of files in output directory:\n" + fileSystem
          .listStatus(tmpOutDir)
          .map(_.getPath)
          .mkString("\n")
      )

    val dataFile = dataFiles.head.getPath

    val targetFile = outDir.resolve(
      systemClock.instant().toString.replaceAll("[:.]", "") + ".snappy.parquet"
    )

    fileSystem.rename(dataFile, targetFile)

    fileSystem.delete(tmpOutDir, true)

    targetFile
  }

  // TODO: Out-of-order tolerance
  // TODO: Idle datasets
  private def getLastWatermark(
    result: Dataset[Row],
    checkpointsDir: Path,
    vocab: DatasetVocabulary
  ): Option[Instant] = {
    if (result.isEmpty) {
      readLastWatermark(checkpointsDir)
    } else {
      val wm = result
        .selectExpr(s"max(cast(`${vocab.eventTimeColumn.get}` as TIMESTAMP))")
        .head()
        .getTimestamp(0)
        .toInstant

      writeLastWatermark(checkpointsDir, wm)
      Some(wm)
    }
  }

  private def writeLastWatermark(
    checkpointDir: Path,
    watermark: Instant
  ): Unit = {
    fileSystem.mkdirs(checkpointDir)
    val outputStream =
      fileSystem.create(checkpointDir.resolve("last_watermark"), true)
    val writer = new PrintWriter(outputStream)

    writer.println(watermark.toString)

    writer.close()
    outputStream.close()
  }

  private def readLastWatermark(checkpointDir: Path): Option[Instant] = {
    if (!fileSystem.exists(checkpointDir))
      return None

    val reader = new Scanner(
      fileSystem.open(checkpointDir.resolve("last_watermark"))
    )
    val watermark = Instant.parse(reader.nextLine())
    reader.close()
    Some(watermark)
  }

  private def computeHash(df: DataFrame): String = {
    if (df.isEmpty)
      return ""
    new DataFrameDigestSHA256().digest(df)
  }

}
