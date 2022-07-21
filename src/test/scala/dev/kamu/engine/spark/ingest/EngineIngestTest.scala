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
import dev.kamu.engine.spark.test.ParquetHelpers
import org.scalatest.{FunSuite, Matchers}

import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.text.SimpleDateFormat
import java.util.Date


case class City(
  date: Date,
  city: String,
  population: Int
)


class EngineIngestTest extends FunSuite with KamuDataFrameSuite with Matchers {
  import spark.implicits._

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
             |datasetID: "did:odf:abcd"
             |datasetName: out
             |ingestPath: "${inputPath}"
             |systemTime: "2020-01-01T00:00:00Z"
             |eventTime: null
             |offset: 10
             |source:
             |  fetch:
             |    kind: url
             |    url: http://localhost
             |  read:
             |    kind: csv
             |    header: true
             |  preprocess:
             |    kind: sql
             |    engine: spark
             |    query: |
             |      select
             |        cast(date as DATE) as date,
             |        city,
             |        cast(population as INT) as population
             |      from input
             |  merge:
             |    kind: ledger
             |    primaryKey:
             |      - date
             |      - city
             |datasetVocab:
             |  eventTimeColumn: date
             |prevCheckpointPath: null
             |newCheckpointPath: "${outputLayout.checkpointsDir}"
             |dataDir: "${outputLayout.dataDir}"
             |outDataPath: "${outputPath}"
             |""".stripMargin
        )

        val engineRunner = new EngineRunner(new DockerClient)
        val response = engineRunner.ingest(request, tempDir)

        response.dataInterval.get shouldEqual OffsetInterval(
          start = 10,
          end = 11
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
          ("date", "date"),
          ("city", "string"),
          ("population", "integer")
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
             |datasetID: "did:odf:abcd"
             |datasetName: out
             |ingestPath: "${inputPath}"
             |systemTime: "2020-01-01T00:00:00Z"
             |eventTime: "2020-01-01T00:00:00Z"
             |offset: 0
             |source:
             |  fetch:
             |    kind: url
             |    url: http://localhost
             |  read:
             |    kind: esriShapefile
             |  merge:
             |    kind: append
             |datasetVocab: {}
             |prevCheckpointPath: null
             |newCheckpointPath: "${outputLayout.checkpointsDir}"
             |dataDir: "${outputLayout.dataDir}"
             |outDataPath: "${outputPath}"
             |""".stripMargin
        )

        val engineRunner = new EngineRunner(new DockerClient)
        val response = engineRunner.ingest(request, tempDir)

        response.dataInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 262
        )

        val df = spark.read.parquet(outputPath.toString)

        df.count() shouldEqual 263
        df.schema.fields
          .map(f => (f.name, f.dataType.typeName))
          .toArray shouldEqual Array(
          ("offset", "long"),
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
             |datasetID: "did:odf:abcd"
             |datasetName: out
             |ingestPath: "${inputPath}"
             |systemTime: "2020-01-01T00:00:00Z"
             |eventTime: "2020-01-01T00:00:00Z"
             |offset: 0
             |source:
             |  fetch:
             |    kind: url
             |    url: http://localhost
             |  read:
             |    kind: geoJson
             |  merge:
             |    kind: append
             |datasetVocab: {}
             |prevCheckpointPath: null
             |newCheckpointPath: "${outputLayout.checkpointsDir}"
             |dataDir: "${outputLayout.dataDir}"
             |outDataPath: "${outputPath}"
             |""".stripMargin
        )

        val engineRunner = new EngineRunner(new DockerClient)
        val response = engineRunner.ingest(request, tempDir)

        response.dataInterval.get shouldEqual OffsetInterval(
          start = 0,
          end = 1
        )

        val df = spark.read.parquet(outputPath.toString)

        df.count() shouldEqual 2
        df.schema.fields
          .map(f => (f.name, f.dataType.typeName))
          .toArray shouldEqual Array(
          ("offset", "long"),
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

  test("ingest Parquet") {
    Temp.withRandomTempDir("kamu-engine-spark")(
      tempDir => {
        val outputLayout = tempLayout(tempDir, "out")

        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

        val inputPath = tempDir.resolve("input1.parquet")
        ParquetHelpers.write[City](
          inputPath,
          Seq(
            City(dateFormat.parse("2020-01-01"), "A", 1000),
            City(dateFormat.parse("2020-01-01"), "B", 2000)
          )
        )

        val outputPath = outputLayout.dataDir.resolve("pending.snappy.parquet")

        val request = yaml.load[IngestRequest](
          s"""
             |datasetID: "did:odf:abcd"
             |datasetName: out
             |ingestPath: "${inputPath}"
             |systemTime: "2020-01-01T00:00:00Z"
             |eventTime: "2020-01-01T00:00:00Z"
             |offset: 0
             |source:
             |  fetch:
             |    kind: url
             |    url: http://localhost
             |  read:
             |    kind: parquet
             |  merge:
             |    kind: append
             |datasetVocab: {}
             |prevCheckpointPath: null
             |newCheckpointPath: "${outputLayout.checkpointsDir}"
             |dataDir: "${outputLayout.dataDir}"
             |outDataPath: "${outputPath}"
             |""".stripMargin
        )

        val engineRunner = new EngineRunner(new DockerClient)
        val response = engineRunner.ingest(request, tempDir)

        response.dataInterval.get shouldEqual OffsetInterval(
          start = 10,
          end = 11
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
          ("date", "date"),
          ("city", "string"),
          ("population", "integer")
        )
      }
    )
  }


}
