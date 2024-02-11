/*
 * Copyright 2018 kamu.dev
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.kamu.engine.spark.test

import java.nio.file.Path
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.{TransformRequest, TransformRequestInput}
import java.sql.Timestamp
import scala.util.Random

trait HasOffset {
  val offset: Long
}

trait EngineHelpers extends DataHelpers {
  def randomFileName(): String = {
    Random.alphanumeric.take(10).mkString("")
  }

  def withRandomOutputPaths(
    request: TransformRequest,
    baseDir: Path,
    prevCheckpointPath: Option[Path] = None
  ): TransformRequest = {
    request.copy(
      newDataPath = baseDir.resolve(randomFileName()),
      prevCheckpointPath = prevCheckpointPath,
      newCheckpointPath = baseDir.resolve(randomFileName())
    )
  }

  def withInputData[T <: HasOffset: Encoder: Decoder](
    request: TransformRequest,
    queryAlias: String,
    dataDir: Path,
    data: Seq[T],
    vocab: DatasetVocabulary = DatasetVocabulary.default()
  )(
    implicit schemaFor: SchemaFor[T]
  ): TransformRequest = {
    val offsetInterval = if (data.nonEmpty) {
      Some(
        OffsetInterval(
          start = data.map(_.offset).min,
          end = data.map(_.offset).max
        )
      )
    } else {
      None
    }

    val inputPath = dataDir.resolve(randomFileName())
    ParquetHelpers.write(
      inputPath,
      data
    )

    request.queryInputs.indexWhere(_.queryAlias == queryAlias) match {
      case -1 =>
        request.copy(
          queryInputs = request.queryInputs ++ Vector(
            TransformRequestInput(
              datasetId = DatasetId("did:odf:" + queryAlias),
              datasetAlias = DatasetAlias(queryAlias),
              queryAlias = queryAlias,
              offsetInterval = offsetInterval,
              schemaFile = inputPath,
              dataPaths = Vector(inputPath),
              explicitWatermarks = Vector.empty,
              vocab = vocab
            )
          )
        )
      case i =>
        val input = request.queryInputs(i)
        val newInput = offsetInterval match {
          case Some(iv) =>
            input.copy(
              dataPaths = input.dataPaths ++ Vector(inputPath),
              offsetInterval = Some(input.offsetInterval.get.copy(end = iv.end))
            )
          case _ => input
        }
        request.copy(queryInputs = request.queryInputs.updated(i, newInput))
    }
  }

  def withWatermarks(
    request: TransformRequest,
    wms: Map[String, Timestamp]
  ): TransformRequest = {
    val wmsVec =
      wms.mapValues(
        eventTime => Vector(Watermark(t(1).toInstant, eventTime.toInstant))
      )

    request.copy(
      queryInputs = request.queryInputs.map(
        i =>
          i.copy(
            explicitWatermarks = wmsVec.getOrElse(i.queryAlias, Vector.empty)
          )
      )
    )
  }

}
