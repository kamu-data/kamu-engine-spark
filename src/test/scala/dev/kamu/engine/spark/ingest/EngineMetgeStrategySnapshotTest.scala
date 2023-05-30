/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest

import pureconfig.generic.auto._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.{DatasetLayout, OffsetInterval}
import dev.kamu.core.manifests.infra.IngestRequest
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.{DockerClient, Temp}
import dev.kamu.engine.spark.{EngineRunner, KamuDataFrameSuite}
import org.scalatest.{FunSuite, Matchers}

import java.nio.file.{Files, Path, Paths}
import java.time.Instant

class EngineMetgeStrategySnapshotTest
    extends FunSuite
    with KamuDataFrameSuite
    with Matchers {
  import spark.implicits._

  def tempLayout(workspaceDir: Path, datasetName: String): DatasetLayout = {
    DatasetLayout(
      metadataDir = workspaceDir.resolve("meta", datasetName),
      dataDir = workspaceDir.resolve("data", datasetName),
      checkpointsDir = workspaceDir.resolve("checkpoints", datasetName),
      cacheDir = workspaceDir.resolve("cache", datasetName)
    )
  }

  test("ingest CSV snapshot") {
    Temp.withRandomTempDir("kamu-engine-spark")(
      tempDir => {
        val outputLayout = tempLayout(tempDir, "out")

        {
          val inputPath = tempDir.resolve("input1.csv")
          val inputData =
            """city,population
              |A,1000
              |B,2000
              |""".stripMargin
          Files.write(inputPath, inputData.getBytes("utf8"))

          val outputPath = outputLayout.dataDir.resolve("data1.snappy.parquet")

          val request = yaml.load[IngestRequest](
            s"""
               |datasetID: "did:odf:abcd"
               |datasetName: out
               |inputDataPath: "${inputPath}"
               |systemTime: "2020-02-01T00:00:00Z"
               |eventTime: "2020-01-01T00:00:00Z"
               |offset: 0
               |source:
               |  fetch:
               |    kind: url
               |    url: http://localhost
               |  read:
               |    kind: csv
               |    header: true
               |    schema:
               |      - city STRING
               |      - population INT
               |  merge:
               |    kind: snapshot
               |    primaryKey:
               |      - city
               |datasetVocab: {}
               |prevCheckpointPath: null
               |newCheckpointPath: "${outputLayout.checkpointsDir}"
               |dataDir: "${outputLayout.dataDir}"
               |outputDataPath: "${outputPath}"
               |""".stripMargin
          )

          val engineRunner = new EngineRunner(new DockerClient)
          val response = engineRunner.ingest(request, tempDir)

          response.dataInterval.get shouldEqual OffsetInterval(
            start = 0,
            end = 1
          )
          response.outputWatermark shouldEqual Some(
            Instant.parse("2020-01-01T00:00:00Z")
          )

          val df = spark.read.parquet(outputPath.toString)
          df.count() shouldEqual 2
          df.schema.fields
            .map(f => (f.name, f.dataType.typeName))
            .toArray shouldEqual Array(
            ("offset", "long"),
            ("system_time", "timestamp"),
            ("event_time", "timestamp"),
            ("observed", "string"),
            ("city", "string"),
            ("population", "integer")
          )
        }

        /////////////////////////////////////////

        {
          val inputPath = tempDir.resolve("input2.csv")
          val inputData =
            """city,population
              |A,1000
              |B,3000
              |""".stripMargin
          Files.write(inputPath, inputData.getBytes("utf8"))

          val outputPath = outputLayout.dataDir.resolve("data2.snappy.parquet")

          val request = yaml.load[IngestRequest](
            s"""
               |datasetID: "did:odf:abcd"
               |datasetName: out
               |inputDataPath: "${inputPath}"
               |systemTime: "2020-02-01T00:00:00Z"
               |eventTime: "2020-01-02T00:00:00Z"
               |offset: 2
               |source:
               |  fetch:
               |    kind: url
               |    url: http://localhost
               |  read:
               |    kind: csv
               |    header: true
               |    schema:
               |      - city STRING
               |      - population INT
               |  merge:
               |    kind: snapshot
               |    primaryKey:
               |      - city
               |datasetVocab: {}
               |prevCheckpointPath: null
               |newCheckpointPath: "${outputLayout.checkpointsDir}"
               |dataDir: "${outputLayout.dataDir}"
               |outputDataPath: "${outputPath}"
               |""".stripMargin
          )

          val engineRunner = new EngineRunner(new DockerClient)
          val response = engineRunner.ingest(request, tempDir)

          response.dataInterval.get shouldEqual OffsetInterval(
            start = 2,
            end = 2
          )
          response.outputWatermark shouldEqual Some(
            Instant.parse("2020-01-02T00:00:00Z")
          )

          val df = spark.read.parquet(outputPath.toString)
          df.count() shouldEqual 1
          df.schema.fields
            .map(f => (f.name, f.dataType.typeName))
            .toArray shouldEqual Array(
            ("offset", "long"),
            ("system_time", "timestamp"),
            ("event_time", "timestamp"),
            ("observed", "string"),
            ("city", "string"),
            ("population", "integer")
          )
        }
      }
    )
  }

}
