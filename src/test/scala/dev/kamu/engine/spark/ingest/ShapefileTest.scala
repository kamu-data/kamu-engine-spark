/*
 * Copyright (c) 2018 kamu.dev
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package dev.kamu.engine.spark.ingest

import dev.kamu.core.manifests.{DatasetSource, MergeStrategy, ReadStep}
import dev.kamu.core.utils.Temp
import dev.kamu.engine.spark.KamuDataFrameSuite
import org.scalatest.{FunSuite, Matchers}

import java.nio.file.{Files, Path, Paths}

class ShapefileTest extends FunSuite with KamuDataFrameSuite with Matchers {

  test("feature per line") {
    Temp.withRandomTempDir("kamu-engine-spark")(
      path => {
        val filePath = Paths.get("test-data/zipcodes.zip")
        if (!Files.exists(filePath)) {
          throw new Exception(
            s"Test data file not found in ${filePath}, perhaps you forgot to run 'make test-data'?"
          )
        }

        val ingest = new Ingest()
        val df =
          ingest.readShapefile(
            spark,
            DatasetSource.Root(
              null,
              None,
              ReadStep.EsriShapefile(None, None),
              None,
              MergeStrategy.Append()
            ),
            filePath
          )

        df.columns shouldEqual Array(
          "geometry",
          "ZIPCODE",
          "BLDGZIP",
          "PO_NAME",
          "POPULATION",
          "AREA",
          "STATE",
          "COUNTY",
          "ST_FIPS",
          "CTY_FIPS",
          "URL",
          "SHAPE_AREA",
          "SHAPE_LEN"
        )
        df.count shouldEqual 263
      }
    )
  }

}
