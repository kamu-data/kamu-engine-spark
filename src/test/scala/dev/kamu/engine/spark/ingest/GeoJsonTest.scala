/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest

import java.nio.file.Files
import dev.kamu.core.utils.{ManualClock, Temp}
import dev.kamu.engine.spark.KamuDataFrameSuite
import org.scalatest.{FunSuite, Matchers}

class GeoJsonTest extends FunSuite with KamuDataFrameSuite with Matchers {
  import spark.implicits._

  test("feature per line") {
    Temp.withRandomTempDir("kamu-engine-spark")(
      path => {
        val geoJson =
          """{"type": "Feature", "properties": {"id": 0, "zipcode": "00101", "name": "A"}, "geometry": {"type": "Polygon", "coordinates": [[[0.0, 0.0],[10.0, 0.0],[10.0, 10.0],[0.0, 10.0],[0.0, 0.0]]]}},
          |{"type": "Feature", "properties": {"id": 1, "zipcode": "00202", "name": "B"}, "geometry": {"type": "Polygon", "coordinates": [[[0.0, 0.0],[20.0, 0.0],[20.0, 20.0],[0.0, 20.0],[0.0, 0.0]]]}}]}
          |""".stripMargin

        val filePath = path.resolve("polygons.json")

        Files.write(filePath, geoJson.getBytes("utf8"))

        val ingest = new Ingest(new ManualClock())
        val df = ingest.readGeoJSON(spark, null, filePath)

        df.count() shouldEqual 2
        df.schema.fields
          .map(f => (f.name, f.dataType.typeName))
          .toArray shouldEqual Array(
          ("geometry", "geometry"),
          ("id", "string"),
          ("zipcode", "string"),
          ("name", "string")
        )
      }
    )
  }

}
