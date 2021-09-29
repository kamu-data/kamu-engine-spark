/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest

import java.io.PrintWriter
import java.nio.file.{Path, Paths}
import java.sql.Timestamp
import java.time.Instant
import java.util.Scanner
import java.util.zip.ZipInputStream
import better.files.File
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.infra.IngestRequest
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import pureconfig.generic.auto._
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.{Clock, ZipFiles}
import dev.kamu.engine.spark.ingest.merge.MergeStrategy
import dev.kamu.engine.spark.ingest.utils.DFUtils._
import dev.kamu.engine.spark.ingest.utils.DataFrameDigestSHA256
import org.apache.log4j.LogManager
import org.apache.sedona.core.formatMapper.GeoJsonReader
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, max}
import org.apache.spark.sql.types.{DataType, DataTypes}
import spire.math.Interval

class Ingest(systemClock: Clock) {
  private val corruptRecordColumn = "__corrupt_record__"
  private val logger = LogManager.getLogger(getClass.getName)
  private val zero_hash =
    "0000000000000000000000000000000000000000000000000000000000000000"

  def ingest(
    spark: SparkSession,
    request: IngestRequest
  ): ExecuteQueryResponse = {
    val block = ingest(
      spark,
      request.source,
      request.eventTime,
      Paths.get(request.ingestPath),
      request.prevCheckpointDir.map(Paths.get(_)),
      Paths.get(request.newCheckpointDir),
      Paths.get(request.dataDir),
      Paths.get(request.outDataPath),
      request.datasetVocab.withDefaults()
    )

    ExecuteQueryResponse.Success(block)
  }

  private def ingest(
    spark: SparkSession,
    source: DatasetSource.Root,
    eventTime: Option[Instant],
    filePath: Path,
    prevCheckpointDir: Option[Path],
    newCheckpointDir: Path,
    dataDir: Path,
    outPath: Path,
    vocab: DatasetVocabulary
  ): MetadataBlock = {
    logger.info(
      s"Ingesting the data: in=$filePath, out=$outPath, format=${source.read}"
    )

    // Needed to make Spark re-read files that might've changed between ingest runs
    spark.sqlContext.clearCache()

    val reader = source.read match {
      case _: ReadStep.EsriShapefile =>
        readShapefile _
      case _: ReadStep.GeoJson =>
        readGeoJSON _
      case _ =>
        readGeneric _
    }

    val result = reader(spark, source, filePath)
      .transform(checkForErrors)
      .transform(preprocess(source))
      .transform(normalizeSchema(source))
      .transform(mergeWithExisting(source, eventTime, dataDir, vocab))
      .transform(postprocess(vocab))

    result.cache()

    val block = MetadataBlock(
      blockHash = zero_hash,
      prevBlockHash = None,
      systemTime = systemClock.instant(),
      outputSlice =
        if (!result.isEmpty)
          Some(
            DataSlice(
              hash = computeHash(result.drop(vocab.systemTimeColumn.get)),
              numRecords = result.count(),
              interval = Interval.point(systemClock.instant())
            )
          )
        else None,
      outputWatermark = getOutputWatermark(result, prevCheckpointDir, vocab)
    )

    if (block.outputWatermark.isDefined) {
      writeLastWatermark(newCheckpointDir, block.outputWatermark.get)
    }

    writeParquet(result, outPath)

    result.unpersist()

    block
  }

  private def readGeneric(
    spark: SparkSession,
    source: DatasetSource.Root,
    filePath: Path
  ): DataFrame = {
    val (name, options) = source.read match {
      case csv: ReadStep.Csv        => ("csv", csv.toSparkReaderOptions)
      case json: ReadStep.JsonLines => ("json", json.toSparkReaderOptions)
      case _ =>
        throw new RuntimeException(s"Not a generic format: ${source.read}")
    }

    val reader = spark.read

    val schema = source.read.schema.getOrElse(Vector.empty)
    if (schema.nonEmpty)
      reader.schema(schema.mkString(", "))

    reader
      .format(name)
      .options(options)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", corruptRecordColumn)
      .load(filePath.toString)
  }

  // TODO: This is inefficient
  private[ingest] def readShapefile(
    spark: SparkSession,
    source: DatasetSource.Root,
    filePath: Path
  ): DataFrame = {
    val fmt = source.read.asInstanceOf[ReadStep.EsriShapefile]

    val extractedPath = filePath.getParent.resolve("shapefile")

    val inputStream = File(filePath).newInputStream
    val zipStream = new ZipInputStream(inputStream)

    ZipFiles.extractZipFile(
      zipStream,
      extractedPath,
      fmt.subPath
    )

    zipStream.close()

    // Shapefiles are sometimes zipped with containing directory, so we check for this case
    val files = extractedPath.toFile.listFiles()
    val actualShapefilePath = if (files.length == 1 && files(0).isDirectory) {
      files(0).toPath
    } else {
      extractedPath
    }

    // FIXME: due to https://issues.apache.org/jira/browse/SEDONA-18 we need to clean up all extra files
    val allowedExtensions = Array("shp", "shx", "dbf", "prj")
    for (f <- actualShapefilePath.toFile.listFiles()) {
      if (!allowedExtensions.contains(f.getName.split('.').last))
        f.delete()
    }

    val rdd = ShapefileReader.readToGeometryRDD(
      spark.sparkContext,
      actualShapefilePath.toString
    )

    Adapter
      .toDf(rdd, spark)
  }

  // TODO: This is very inefficient, should extend GeoSpark to support this
  private[ingest] def readGeoJSON(
    spark: SparkSession,
    source: DatasetSource.Root,
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
    source: DatasetSource.Root
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

  private def preprocess(
    source: DatasetSource.Root
  )(df: DataFrame): DataFrame = {
    if (source.preprocess.isEmpty)
      return df

    if (source.preprocess.get.engine != "spark")
      throw new RuntimeException(
        s"Unsupported engine: ${source.preprocess.get.engine}"
      )

    val transformRaw = source.preprocess.get.asInstanceOf[Transform.Sql]

    val transform = transformRaw.copy(
      queries =
        if (transformRaw.query.isDefined)
          Some(Vector(SqlQueryStep(None, transformRaw.query.get)))
        else transformRaw.queries
    )

    val spark = df.sparkSession
    df.createTempView("input")

    for (step <- transform.queries.get) {
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
    source: DatasetSource.Root,
    eventTime: Option[Instant],
    dataDir: Path,
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
    val prev = Some(dataDir)
      .filter(p => File(p).exists)
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

  private def writeParquet(df: DataFrame, outPath: Path): Unit = {
    val outDir = outPath.getParent
    val tmpOutDir = outDir.resolve(".tmp")

    df.write.parquet(tmpOutDir.toString)

    val dataFiles = File(tmpOutDir).glob("*.snappy.parquet").toList

    if (dataFiles.length != 1)
      throw new RuntimeException(
        "Unexpected number of files in output directory:\n" + File(tmpOutDir).list
          .map(_.path.toString)
          .mkString("\n")
      )

    val dataFile = dataFiles.head.path

    File(dataFile).moveTo(outPath)
    File(tmpOutDir).delete()
  }

  // TODO: Out-of-order tolerance
  // TODO: Idle datasets
  private[ingest] def getOutputWatermark(
    result: Dataset[Row],
    prevCheckpointDir: Option[Path],
    vocab: DatasetVocabulary
  ): Option[Instant] = {
    if (result.isEmpty) {
      prevCheckpointDir.flatMap(readLastWatermark)
    } else {
      val wm_type = result.schema(vocab.eventTimeColumn.get).dataType

      if (!Array("timestamp", "date").contains(wm_type.typeName))
        throw new RuntimeException(
          s"Event time column can only be TIMESTAMP or DATE, got: ${wm_type.typeName}"
        )

      val wm_col = result.col(vocab.eventTimeColumn.get)

      val has_nulls = result.select("*").where(wm_col.isNull).limit(1)
      if (has_nulls.count() != 0)
        throw new RuntimeException(
          s"Event time column cannot have NULLs, sample row: ${has_nulls.head()}"
        )

      val max_wm = result
        .agg(max(wm_col.cast(DataTypes.TimestampType)))
        .head()
        .getTimestamp(0)

      Some(max_wm.toInstant)
    }
  }

  private def writeLastWatermark(
    checkpointDir: Path,
    watermark: Instant
  ): Unit = {
    File(checkpointDir).createDirectories()
    val outputStream = File(checkpointDir.resolve("last_watermark")).newOutputStream
    val writer = new PrintWriter(outputStream)

    writer.println(watermark.toString)

    writer.close()
    outputStream.close()
  }

  private def readLastWatermark(checkpointDir: Path): Option[Instant] = {
    if (!File(checkpointDir).exists)
      return None

    val reader = new Scanner(
      File(checkpointDir.resolve("last_watermark")).newInputStream
    )
    val watermark = Instant.parse(reader.nextLine())
    reader.close()
    Some(watermark)
  }

  private def computeHash(df: DataFrame): String = {
    if (df.isEmpty)
      return zero_hash
    new DataFrameDigestSHA256().digest(df)
  }

}
