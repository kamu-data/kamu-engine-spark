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
import org.scalatest.funsuite._
import org.scalatest.matchers.should.Matchers

case class AnsiTest(
  s: String
)

class EngineAnsiTest extends AnyFunSuite with EngineHelpers with Matchers {
  test("Runs in ANSI mode") {
    Temp.withRandomTempDir("kamu-engine-spark-") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      //val inputDir = tempDir.resolve("in")
      val outputPath = tempDir.resolve("out")

      var request = yaml.load[RawQueryRequest](
        s"""
           |transform:
           |  kind: Sql
           |  engine: spark
           |  queries:
           |  - query:  select cast(unix_timestamp("test") as string) as s
           |inputDataPaths: []
           |outputDataPath: $outputPath
           |""".stripMargin
      )

      try {
        engineRunner.rawQuery(request, tempDir)
        fail("Ansi mode seems to be not enabled")
      } catch {
        case _: Throwable => ()
      }

      request = yaml.load[RawQueryRequest](
        s"""
           |transform:
           |  kind: Sql
           |  engine: spark
           |  queries:
           |  - query:  select cast(unix_timestamp("2020-01-01 00:00:00") as string) as s
           |inputDataPaths: []
           |outputDataPath: $outputPath
           |""".stripMargin
      )
      engineRunner.rawQuery(request, tempDir)

      val actual = ParquetHelpers
        .read[AnsiTest](outputPath)

      actual shouldEqual List(
        AnsiTest("1577836800")
      )
    }
  }
}
