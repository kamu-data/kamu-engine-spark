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
import java.time.{Instant, LocalDate}
import java.text.SimpleDateFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

case class CityEvent(
  date: LocalDate,
  city: String,
  population: Int
)

class EngineIngestTest extends FunSuite with KamuDataFrameSuite with Matchers {
  import spark.implicits._

  def tempLayout(workspaceDir: Path, datasetName: String): DatasetLayout = {
    DatasetLayout(
      metadataDir = workspaceDir.resolve("meta", datasetName),
      dataDir = workspaceDir.resolve("data", datasetName),
      checkpointsDir = workspaceDir.resolve("checkpoints", datasetName)
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
             |datasetId: "did:odf:abcd"
             |datasetAlias: out
             |inputDataPath: "${inputPath}"
             |systemTime: "2020-01-01T00:00:00Z"
             |eventTime: null
             |nextOffset: 10
             |source:
             |  fetch:
             |    kind: Url
             |    url: http://localhost
             |  read:
             |    kind: Csv
             |    header: true
             |  preprocess:
             |    kind: Sql
             |    engine: spark
             |    query: |
             |      select
             |        cast(date as DATE) as date,
             |        city,
             |        cast(population as INT) as population
             |      from input
             |  merge:
             |    kind: Ledger
             |    primaryKey:
             |      - date
             |      - city
             |datasetVocab:
             |  eventTimeColumn: date
             |prevCheckpointPath: null
             |newCheckpointPath: "${outputLayout.checkpointsDir}"
             |dataDir: "${outputLayout.dataDir}"
             |outputDataPath: "${outputPath}"
             |""".stripMargin
        )

        val engineRunner = new EngineRunner(new DockerClient)
        val response = engineRunner.ingest(request, tempDir)

        response.newOffsetInterval.get shouldEqual OffsetInterval(
          start = 10,
          end = 11
        )
        response.newWatermark shouldEqual Some(
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
             |datasetId: "did:odf:abcd"
             |datasetAlias: out
             |inputDataPath: "${inputPath}"
             |systemTime: "2020-01-01T00:00:00Z"
             |eventTime: "2020-01-01T00:00:00Z"
             |nextOffset: 0
             |source:
             |  fetch:
             |    kind: Url
             |    url: http://localhost
             |  read:
             |    kind: EsriShapefile
             |  merge:
             |    kind: Append
             |datasetVocab: {}
             |prevCheckpointPath: null
             |newCheckpointPath: "${outputLayout.checkpointsDir}"
             |dataDir: "${outputLayout.dataDir}"
             |outputDataPath: "${outputPath}"
             |""".stripMargin
        )

        val engineRunner = new EngineRunner(new DockerClient)
        val response = engineRunner.ingest(request, tempDir)

        response.newOffsetInterval.get shouldEqual OffsetInterval(
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
             |datasetId: "did:odf:abcd"
             |datasetAlias: out
             |inputDataPath: "${inputPath}"
             |systemTime: "2020-01-01T00:00:00Z"
             |eventTime: "2020-01-01T00:00:00Z"
             |nextOffset: 0
             |source:
             |  fetch:
             |    kind: Url
             |    url: http://localhost
             |  read:
             |    kind: NdGeoJson
             |  merge:
             |    kind: Append
             |datasetVocab: {}
             |prevCheckpointPath: null
             |newCheckpointPath: "${outputLayout.checkpointsDir}"
             |dataDir: "${outputLayout.dataDir}"
             |outputDataPath: "${outputPath}"
             |""".stripMargin
        )

        val engineRunner = new EngineRunner(new DockerClient)
        val response = engineRunner.ingest(request, tempDir)

        response.newOffsetInterval.get shouldEqual OffsetInterval(
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

        val inputPath = tempDir.resolve("read.bin")
        ParquetHelpers.write(
          inputPath,
          Seq(
            CityEvent(LocalDate.parse("2020-01-01"), "A", 1000),
            CityEvent(LocalDate.parse("2020-02-01"), "B", 2000)
          ),
          CompressionCodecName.UNCOMPRESSED
        )

        val outputPath = outputLayout.dataDir.resolve("pending.snappy.parquet")

        val request = yaml.load[IngestRequest](
          s"""
             |datasetId: "did:odf:abcd"
             |datasetAlias: out
             |inputDataPath: "${inputPath}"
             |systemTime: "2020-01-01T00:00:00Z"
             |eventTime: "2020-01-01T00:00:00Z"
             |nextOffset: 10
             |source:
             |  fetch:
             |    kind: Url
             |    url: http://localhost
             |  read:
             |    kind: Parquet
             |  merge:
             |    kind: Append
             |datasetVocab:
             |  eventTimeColumn: date
             |prevCheckpointPath: null
             |newCheckpointPath: "${outputLayout.checkpointsDir}"
             |dataDir: "${outputLayout.dataDir}"
             |outputDataPath: "${outputPath}"
             |""".stripMargin
        )

        val engineRunner = new EngineRunner(new DockerClient)
        val response = engineRunner.ingest(request, tempDir)

        response.newOffsetInterval.get shouldEqual OffsetInterval(
          start = 10,
          end = 11
        )
        response.newWatermark shouldEqual Some(
          Instant.parse("2020-02-01T00:00:00Z")
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
