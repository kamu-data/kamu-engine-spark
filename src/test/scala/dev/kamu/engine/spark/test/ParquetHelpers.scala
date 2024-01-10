/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.test

import java.nio.file.Path
import com.sksamuel.avro4s.{AvroSchema, Decoder, Encoder, RecordFormat, SchemaFor}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.MessageType

object ParquetHelpers {
  def write[T: Encoder: Decoder](
    path: Path,
    data: Seq[T],
    compressionCodec: CompressionCodecName = CompressionCodecName.SNAPPY
  )(
    implicit schemaFor: SchemaFor[T]
  ): Unit = {
    val avroSchema = AvroSchema[T]
    val format = RecordFormat[T]

    val records = data.map(format.to)

    println(avroSchema.toString(true))

    val writer = AvroParquetWriter
      .builder[GenericRecord](new org.apache.hadoop.fs.Path(path.toUri))
      .withSchema(avroSchema)
      .withDataModel(GenericData.get)
      .withCompressionCodec(compressionCodec)
      .build()

    records.foreach(writer.write)

    writer.close()
  }

  def read[T: Encoder: Decoder](path: Path)(
    implicit schemaFor: SchemaFor[T]
  ): List[T] = {
    val format = RecordFormat[T]

    val reader =
      AvroParquetReader
        .builder[GenericRecord](new org.apache.hadoop.fs.Path(path.toUri))
        .withDataModel(GenericData.get)
        .build()

    val records = Stream
      .continually(reader.read)
      .takeWhile(_ != null)
      .map(format.from)
      .toList

    reader.close()
    records
  }

  def getSchemaFromFile(path: Path): MessageType = {
    val file = HadoopInputFile.fromPath(
      new org.apache.hadoop.fs.Path(path.toUri),
      new org.apache.hadoop.conf.Configuration()
    )
    val reader = ParquetFileReader.open(file)
    val schema = reader.getFileMetaData.getSchema
    reader.close()
    schema
  }
}
