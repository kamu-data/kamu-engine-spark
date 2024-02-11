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

import dev.kamu.core.manifests._
import dev.kamu.core.utils._
import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.engine.spark.Op
import org.scalatest.funsuite._
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.sql.Timestamp

case class TickerRaw(
  offset: Long,
  op: Int,
  system_time: Timestamp,
  event_time: Timestamp,
  symbol: String,
  value: Long
) extends HasOffset

class EngineMapTest extends AnyFunSuite with EngineHelpers with Matchers {
  test("Map - basic") {
    Temp.withRandomTempDir("kamu-engine-spark-") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val inputDir = tempDir.resolve("in")
      val outputDir = tempDir.resolve("out")

      val requestTemplate = yaml.load[TransformRequest](
        s"""
           |datasetId: "did:odf:blah"
           |datasetAlias: out
           |systemTime: "2020-01-01T00:00:00Z"
           |nextOffset: 0
           |transform:
           |  kind: Sql
           |  engine: spark
           |  queries:
           |  - query: |
           |      SELECT
           |        op,
           |        event_time,
           |        symbol,
           |        value * 10 as value
           |      FROM `in`
           |queryInputs: []
           |newCheckpointPath: ""
           |newDataPath: ""
           |vocab:
           |  offsetColumn: offset
           |  operationTypeColumn: op
           |  systemTimeColumn: system_time
           |  eventTimeColumn: event_time
           |""".stripMargin
      )

      var checkpointDir = {
        var request = withRandomOutputPaths(requestTemplate, outputDir)
        request = withInputData(
          request,
          "in",
          inputDir,
          Seq(
            TickerRaw(0, Op.Append, t(5), t(1), "A", 10),
            TickerRaw(1, Op.Append, t(5), t(1), "B", 20),
            TickerRaw(2, Op.Append, t(5), t(2), "A", 11),
            TickerRaw(3, Op.Append, t(5), t(2), "B", 21)
          )
        )

        val result = engineRunner.executeTransform(
          withWatermarks(request, Map("in" -> t(2)))
            .copy(systemTime = t(10).toInstant, nextOffset = 0),
          tempDir
        )

        val actual = ParquetHelpers
          .read[TickerRaw](request.newDataPath)
          .sortBy(_.offset)

        actual shouldEqual List(
          TickerRaw(0, Op.Append, t(10), t(1), "A", 100),
          TickerRaw(1, Op.Append, t(10), t(1), "B", 200),
          TickerRaw(2, Op.Append, t(10), t(2), "A", 110),
          TickerRaw(3, Op.Append, t(10), t(2), "B", 210)
        )

        ParquetHelpers
          .getSchemaFromFile(request.newDataPath)
          .toString
          .trim() shouldEqual
          """message spark_schema {
            |  required int64 offset;
            |  optional int32 op;
            |  required int64 system_time (TIMESTAMP(MILLIS,true));
            |  optional int64 event_time (TIMESTAMP(MILLIS,true));
            |  optional binary symbol (STRING);
            |  optional int64 value;
            |}""".stripMargin

        result.newOffsetInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 3
        )
        result.newWatermark.get shouldEqual t(2).toInstant

        request.newCheckpointPath
      }

      // Input comes in two files
      checkpointDir = {
        var request =
          withRandomOutputPaths(requestTemplate, outputDir, Some(checkpointDir))

        request = withInputData(
          request,
          "in",
          inputDir,
          Seq(
            TickerRaw(4, Op.Append, t(6), t(3), "A", 12),
            TickerRaw(5, Op.Append, t(6), t(3), "B", 22)
          )
        )

        request = withInputData(
          request,
          "in",
          inputDir,
          Seq(
            TickerRaw(6, Op.Append, t(7), t(4), "A", 13),
            TickerRaw(7, Op.Append, t(7), t(4), "B", 23)
          )
        )

        val result = engineRunner.executeTransform(
          withWatermarks(request, Map("in" -> t(4)))
            .copy(systemTime = t(11).toInstant, nextOffset = 4),
          tempDir
        )

        val actual = ParquetHelpers
          .read[TickerRaw](request.newDataPath)
          .sortBy(_.offset)

        actual shouldEqual List(
          TickerRaw(4, Op.Append, t(11), t(3), "A", 120),
          TickerRaw(5, Op.Append, t(11), t(3), "B", 220),
          TickerRaw(6, Op.Append, t(11), t(4), "A", 130),
          TickerRaw(7, Op.Append, t(11), t(4), "B", 230)
        )

        result.newOffsetInterval.get shouldEqual OffsetInterval(
          start = 4,
          end = 7
        )
        result.newWatermark.get shouldEqual t(4).toInstant

        request.newCheckpointPath
      }

      // No data, only watermark
      {
        var request =
          withRandomOutputPaths(requestTemplate, outputDir, Some(checkpointDir))

        request = withInputData(
          request,
          "in",
          inputDir,
          Seq.empty[TickerRaw]
        )

        val result = engineRunner.executeTransform(
          withWatermarks(request, Map("in" -> t(5)))
            .copy(systemTime = t(12).toInstant, nextOffset = 8),
          tempDir
        )

        Files.exists(request.newDataPath) shouldEqual false
        result.newOffsetInterval shouldEqual None
        result.newWatermark.get shouldEqual t(5).toInstant

        request.newCheckpointPath
      }

      // Retractions corrections
      {
        var request =
          withRandomOutputPaths(requestTemplate, outputDir, Some(checkpointDir))

        request = withInputData(
          request,
          "in",
          inputDir,
          Seq(
            TickerRaw(8, Op.CorrectFrom, t(8), t(3), "A", 12),
            TickerRaw(9, Op.CorrectTo, t(8), t(3), "A", 13)
          )
        )

        request = withInputData(
          request,
          "in",
          inputDir,
          Seq(
            TickerRaw(10, Op.Retract, t(9), t(2), "A", 11),
            TickerRaw(11, Op.Retract, t(9), t(2), "B", 21)
          )
        )

        val result = engineRunner.executeTransform(
          withWatermarks(request, Map("in" -> t(5)))
            .copy(systemTime = t(13).toInstant, nextOffset = 8),
          tempDir
        )

        val actual = ParquetHelpers
          .read[TickerRaw](request.newDataPath)
          .sortBy(_.offset)

        actual shouldEqual List(
          TickerRaw(8, Op.CorrectFrom, t(13), t(3), "A", 120),
          TickerRaw(9, Op.CorrectTo, t(13), t(3), "A", 130),
          TickerRaw(10, Op.Retract, t(13), t(2), "A", 110),
          TickerRaw(11, Op.Retract, t(13), t(2), "B", 210)
        )

        result.newOffsetInterval.get shouldEqual OffsetInterval(
          start = 8,
          end = 11
        )
        result.newWatermark.get shouldEqual t(5).toInstant

        request.newCheckpointPath
      }
    }
  }
}
