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
import dev.kamu.core.manifests.DatasetLayout
import dev.kamu.core.manifests.infra.IngestRequest
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.{DockerClient, ManualClock, Temp}
import dev.kamu.engine.spark.{EngineRunner, KamuDataFrameSuite}
import org.scalatest.{FunSuite, Matchers}

import java.nio.file.{Files, Path, Paths}
import java.time.Instant

class EngineIngestTest extends FunSuite with KamuDataFrameSuite with Matchers {

  def tempLayout(workspaceDir: Path, datasetName: String): DatasetLayout = {
    DatasetLayout(
      metadataDir = workspaceDir.resolve("meta", datasetName),
      dataDir = workspaceDir.resolve("data", datasetName),
      checkpointsDir = workspaceDir.resolve("checkpoints", datasetName),
      cacheDir = workspaceDir.resolve("cache", datasetName)
    )
  }

  test("ingest CSV") {
    Temp.withRandomTempDir("kamu-engine-spark")(
      tempDir => {
        val outputLayout = tempLayout(tempDir, "out")

        val inputPath = tempDir.resolve("input1.csv")
        val inputData =
          """date,city,population
            |2020-01-01,A,1000
            |2020-01-01,B,2000
            |""".stripMargin
        Files.write(inputPath, inputData.getBytes("utf8"))

        val outputPath = outputLayout.dataDir.resolve("pending.snappy.parquet")

        val request = yaml.load[IngestRequest](
          s"""
             |datasetID: out
             |ingestPath: "${inputPath}"
             |eventTime: null
             |source:
             |  fetch:
             |    kind: url
             |    url: http://localhost
             |  read:
             |    kind: csv
             |    header: true
             |  merge:
             |    kind: ledger
             |    primaryKey:
             |      - date
             |      - city
             |datasetVocab:
             |  eventTimeColumn: date
             |prevCheckpointDir: null
             |newCheckpointDir: "${outputLayout.checkpointsDir}"
             |dataDir: "${outputLayout.dataDir}"
             |outDataPath: "${outputPath}"
             |""".stripMargin
        )

        val engineRunner = new EngineRunner(new DockerClient)
        val response = engineRunner.ingest(request, tempDir)

        response.block.outputSlice.get.numRecords shouldEqual 2
        response.block.outputSlice.get.hash shouldEqual "ec3c5e8abbefeadff9b0e3897e3c3c969d9e8de734c06d08f1b0296c194ca58f"
        response.block.outputWatermark shouldEqual Some(
          Instant.parse("2020-01-01T00:00:00Z")
        )
      }
    )
  }

  test("ingest Shapefile") {
    Temp.withRandomTempDir("kamu-engine-spark")(
      tempDir => {
        val outputLayout = tempLayout(tempDir, "out")

        val filePath = Paths.get("test-data/zipcodes.zip")
        if (!Files.exists(filePath)) {
          throw new Exception(
            s"Test data file not found in ${filePath}, perhaps you forgot to run 'make test-data'?"
          )
        }

        val inputPath = tempDir.resolve("input1.zip")
        Files.copy(filePath, inputPath)

        val outputPath = outputLayout.dataDir.resolve("pending.snappy.parquet")

        val request = yaml.load[IngestRequest](
          s"""
             |datasetID: out
             |ingestPath: "${inputPath}"
             |eventTime: "2020-01-01T00:00:00Z"
             |source:
             |  fetch:
             |    kind: url
             |    url: http://localhost
             |  read:
             |    kind: esriShapefile
             |  merge:
             |    kind: append
             |datasetVocab: {}
             |prevCheckpointDir: null
             |newCheckpointDir: "${outputLayout.checkpointsDir}"
             |dataDir: "${outputLayout.dataDir}"
             |outDataPath: "${outputPath}"
             |""".stripMargin
        )

        val engineRunner = new EngineRunner(new DockerClient)
        val response = engineRunner.ingest(request, tempDir)

        response.block.outputSlice.get.numRecords shouldEqual 263
        response.block.outputSlice.get.hash shouldEqual "265d533419048f85c73e5e37844c1088b03bfdb18f6873ed19f2de13abed72b3"

        val df = spark.read.parquet(outputPath.toString)

        df.count() shouldEqual 263
        df.schema.fields
          .map(f => (f.name, f.dataType.typeName))
          .toArray shouldEqual Array(
          ("system_time", "timestamp"),
          ("event_time", "timestamp"),
          ("geometry", "geometry"),
          ("ZIPCODE", "string"),
          ("BLDGZIP", "string"),
          ("PO_NAME", "string"),
          ("POPULATION", "string"),
          ("AREA", "string"),
          ("STATE", "string"),
          ("COUNTY", "string"),
          ("ST_FIPS", "string"),
          ("CTY_FIPS", "string"),
          ("URL", "string"),
          ("SHAPE_AREA", "string"),
          ("SHAPE_LEN", "string")
        )
      }
    )
  }

  test("ingest GeoJson") {
    Temp.withRandomTempDir("kamu-engine-spark")(
      tempDir => {
        val outputLayout = tempLayout(tempDir, "out")

        val inputPath = tempDir.resolve("input1.geojson")
        val inputData =
          """{"type": "Feature", "properties": {"id": 0, "zipcode": "00101", "name": "A"}, "geometry": {"type": "Polygon", "coordinates": [[[0.0, 0.0],[10.0, 0.0],[10.0, 10.0],[0.0, 10.0],[0.0, 0.0]]]}},
            |{"type": "Feature", "properties": {"id": 1, "zipcode": "00202", "name": "B"}, "geometry": {"type": "Polygon", "coordinates": [[[0.0, 0.0],[20.0, 0.0],[20.0, 20.0],[0.0, 20.0],[0.0, 0.0]]]}}]}
            |""".stripMargin
        Files.write(inputPath, inputData.getBytes("utf8"))

        val outputPath = outputLayout.dataDir.resolve("pending.snappy.parquet")

        val request = yaml.load[IngestRequest](
          s"""
             |datasetID: out
             |ingestPath: "${inputPath}"
             |eventTime: "2020-01-01T00:00:00Z"
             |source:
             |  fetch:
             |    kind: url
             |    url: http://localhost
             |  read:
             |    kind: geoJson
             |  merge:
             |    kind: append
             |datasetVocab: {}
             |prevCheckpointDir: null
             |newCheckpointDir: "${outputLayout.checkpointsDir}"
             |dataDir: "${outputLayout.dataDir}"
             |outDataPath: "${outputPath}"
             |""".stripMargin
        )

        val engineRunner = new EngineRunner(new DockerClient)
        val response = engineRunner.ingest(request, tempDir)

        response.block.outputSlice.get.numRecords shouldEqual 2
        response.block.outputSlice.get.hash shouldEqual "34c9c4bd5bdfd5d556f3cae3b72187fc7173b969201900d4ab00e93ae10af6bb"

        val df = spark.read.parquet(outputPath.toString)

        df.count() shouldEqual 2
        df.schema.fields
          .map(f => (f.name, f.dataType.typeName))
          .toArray shouldEqual Array(
          ("system_time", "timestamp"),
          ("event_time", "timestamp"),
          ("geometry", "geometry"),
          ("id", "string"),
          ("zipcode", "string"),
          ("name", "string")
        )
      }
    )
  }

}
