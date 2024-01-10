/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark

import java.io.PrintWriter
import java.nio.file.Path
import java.sql.Timestamp
import java.time.Instant
import java.util.Scanner
import better.files.File
import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests._
import dev.kamu.core.utils.fs._
import DFUtils._
import org.apache.log4j.LogManager
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{OriginalType, PrimitiveType, Types}
import org.apache.spark.sql.expressions.Window

import collection.JavaConverters._
import org.apache.spark.sql.functions.{
  lit,
  monotonically_increasing_id,
  row_number
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Some logic eventually should be moved to the coordinator side **/
class Transform(
  spark: SparkSession
) {
  private val outputViewName = "__output__"
  private val orderColumnName = "__order__"
  private val logger = LogManager.getLogger(getClass.getName)

  def execute(
    request: TransformRequest
  ): TransformResponse = {
    val transform = request.transform.asInstanceOf[Transform.Sql]
    assert(transform.engine.toLowerCase == "spark")

    val inputWatermarks =
      getInputWatermarks(request.queryInputs, request.prevCheckpointPath)

    // Setup inputs
    val dataFrames = request.queryInputs
      .map(input => {
        val df = prepareInputDataframe(spark, input)
        df.cache()
        df.createTempView(s"`${input.queryAlias}`")
        df
      })

    // Setup transform
    for (step <- transform.queries.get) {
      val name = step.alias.getOrElse(outputViewName)
      spark
        .sql(step.query)
        .createTempView(s"`$name`")
    }

    // Process data
    var result = spark.sql(s"SELECT * FROM `$outputViewName`")

    val vocab = request.vocab

    for (c <- Array(vocab.offsetColumn, vocab.systemTimeColumn))
      if (result.getColumn(c).isDefined)
        throw new Exception(
          s"Transformed data contains column '$c' that conflicts with system column name, " +
            s"you should either rename the data column or configure the dataset vocabulary " +
            s"to use a different name"
        )

    if (result.getColumn(vocab.eventTimeColumn).isEmpty)
      throw new Exception(
        s"Transformed data does not contains an event time column: ${vocab.eventTimeColumn}"
      )

    val eventTimeType =
      result.schema(vocab.eventTimeColumn).dataType.typeName
    if (!Array("timestamp", "date").contains(eventTimeType))
      throw new RuntimeException(
        s"Event time column can only be TIMESTAMP or DATE, got: $eventTimeType"
      )

    // Assign operation type as append if not carried through transformation
    result =
      if (!result.hasColumn(vocab.operationTypeColumn))
        result.withColumn(vocab.operationTypeColumn, lit(Op.Append))
      else {
        val opType =
          result.schema(vocab.operationTypeColumn).dataType.typeName
        if (opType != "integer")
          throw new RuntimeException(
            s"Operation type column has to be INT, got: $opType"
          )
        result
      }

    result = result
      .withColumn(orderColumnName, monotonically_increasing_id)
      .withColumn(
        vocab.offsetColumn,
        row_number
          .over(Window.orderBy(orderColumnName)) + (request.nextOffset - 1)
      )
      .drop(orderColumnName)
      .withColumn(
        vocab.systemTimeColumn,
        lit(Timestamp.from(request.systemTime))
      )
      .columnToFront(
        vocab.offsetColumn,
        vocab.operationTypeColumn,
        vocab.systemTimeColumn
      )

    result.cache()

    // Write input watermarks in case they will not be passed during next run
    for ((datasetId, watermark) <- inputWatermarks) {
      writeWatermark(datasetId, request.newCheckpointPath, watermark)
    }

    // Write data
    if (!result.isEmpty)
      result.writeParquetSingleFile(request.newDataPath)

    val newOffsetInterval =
      if (!result.isEmpty)
        Some(
          OffsetInterval(
            start = request.nextOffset,
            end = request.nextOffset + result.count() - 1
          )
        )
      else None

    val newWatermark = Some(inputWatermarks.values.min)

    // Release memory
    result.unpersist(true)
    dataFrames.foreach(_.unpersist(true))

    TransformResponse.Success(
      newOffsetInterval = newOffsetInterval,
      newWatermark = newWatermark
    )
  }

  private def prepareInputDataframe(
    spark: SparkSession,
    input: TransformRequestInput
  ): DataFrame = {
    val schema = readSchemaNormalized(spark, input.schemaFile)

    val df = spark.read
      .format("parquet")
      .schema(schema)
      .parquet(input.dataPaths.map(_.toString): _*)

    val subset = input.offsetInterval match {
      case None =>
        df.where(lit(false))
      case Some(iv) =>
        val col = df.col(input.vocab.offsetColumn)
        df.filter(col >= iv.start && col <= iv.end)
    }

    // TODO: This limits parallelism, but ensures that map/filter processing of the input also results
    //  in one partition and preserves the order of events, which is important in case of retractions/corrections.
    subset.coalesce(1).orderBy(input.vocab.offsetColumn)
  }

  // TODO: This loads parquet schema from a file.
  //  Because Spark does not support integer logical types like `(INTEGER(8, true))` we need to preprocess the schema
  //  to avoid a crash. We should revisit this after upgrading to latest version of spark.
  private def readSchemaNormalized(
    spark: SparkSession,
    schemaFile: Path
  ): StructType = {
    val reader = org.apache.parquet.hadoop.ParquetFileReader
      .open(
        HadoopInputFile.fromPath(
          new org.apache.hadoop.fs.Path(schemaFile.toString),
          spark.sparkContext.hadoopConfiguration
        )
      )

    val parquetSchema = reader.getFileMetaData.getSchema

    val converter =
      new org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter()

    logger.info(s"Read input parquet schema: $parquetSchema")

    val builder = Types.buildMessage()
    for ((t, i) <- parquetSchema.getFields.asScala.zipWithIndex) {
      val name = parquetSchema.getFieldName(i)
      val newTyp = {
        // Detect and erase INT(X, unsigned) annotations
        if (Set(
              OriginalType.UINT_8,
              OriginalType.UINT_16,
              OriginalType.UINT_32,
              OriginalType.UINT_64
            ).contains(t.asPrimitiveType().getOriginalType)) {
          new PrimitiveType(
            t.getRepetition,
            t.asPrimitiveType().getPrimitiveTypeName,
            t.getName
            // Erasing OriginalType
          )
        } else {
          t
        }
      }
      builder.addField(newTyp).named(name)
    }

    val newParquetSchema = builder.named(parquetSchema.getName)
    logger.info(s"Normalized input parquet schema: $newParquetSchema")

    converter.convert(newParquetSchema)
  }

  private def getInputWatermarks(
    inputs: Vector[TransformRequestInput],
    prevCheckpointDir: Option[Path]
  ): Map[DatasetId, Instant] = {
    val previousWatermarks: Map[DatasetId, Instant] =
      if (prevCheckpointDir.isDefined) {
        inputs
          .map(
            input =>
              (
                input.datasetId,
                readWatermark(input.datasetId, prevCheckpointDir.get)
              )
          )
          .toMap
      } else {
        Map.empty
      }

    inputs
      .map(input => {
        (
          input.datasetId,
          maxOption(input.explicitWatermarks.map(_.eventTime))
            .getOrElse(previousWatermarks(input.datasetId))
        )
      })
      .toMap
  }

  private def readWatermark(
    datasetId: DatasetId,
    checkpointDir: Path
  ): Instant = {
    val wmPath = checkpointDir.resolve(s"${datasetId.toMultibase()}.watermark")
    val reader = new Scanner(File(wmPath).newInputStream)
    val watermark = Instant.parse(reader.nextLine())
    reader.close()
    watermark
  }

  private def writeWatermark(
    datasetId: DatasetId,
    checkpointDir: Path,
    watermark: Instant
  ): Unit = {
    File(checkpointDir).createDirectories()
    val outputStream = File(
      checkpointDir.resolve(s"${datasetId.toMultibase()}.watermark")
    ).newOutputStream
    val writer = new PrintWriter(outputStream)
    writer.println(watermark.toString)
    writer.close()
    outputStream.close()
  }

  private def maxOption[T: Ordering](seq: Seq[T]): Option[T] = {
    if (seq.isEmpty)
      None
    else
      Some(seq.max)
  }

}
