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

package dev.kamu.engine.spark

import dev.kamu.core.utils.fs._
import org.apache.spark.sql.{
  Column,
  DFHelper,
  DataFrame,
  RelationalGroupedDataset
}

import java.nio.file.Path

object DFUtils {

  implicit class DataFrameEx(val df: DataFrame) {

    def showString(
      numRows: Int = 20,
      truncate: Int = 20,
      vertical: Boolean = false
    ): String = {
      DFHelper.showString(df, numRows, truncate, vertical)
    }

    def maybeTransform(
      condition: Boolean,
      tr: DataFrame => DataFrame
    ): DataFrame = {
      if (condition)
        df.transform(tr)
      else
        df
    }

    /** Get column if exists
      *
      * TODO: Will not work with nested columns
      */
    def getColumn(column: String): Option[Column] = {
      if (df.columns.contains(column))
        Some(df(column))
      else
        None
    }

    def hasColumn(column: String): Boolean = {
      df.getColumn(column).isDefined
    }

    /** Reorders columns in the [[DataFrame]] */
    def columnToFront(columns: String*): DataFrame = {
      val front = columns.toList
      val back = df.columns.filter(!front.contains(_))
      val newColumns = front ++ back
      val head :: tail = newColumns
      df.select(head, tail: _*)
    }

    /** Reorders columns in the [[DataFrame]] */
    def columnToBack(columns: String*): DataFrame = {
      val back = columns.toList
      val front = df.columns.filter(!back.contains(_)).toList
      val newColumns = front ++ back
      val head :: tail = newColumns
      df.select(head, tail: _*)
    }

    def writeParquetSingleFile(outPath: Path): Unit = {
      val outDir = outPath.getParent
      val tmpOutDir = outDir.resolve(".tmp")

      df.write.parquet(tmpOutDir.toString)

      val dataFiles = tmpOutDir.toFile
        .listFiles()
        .filter(_.getPath.endsWith(".snappy.parquet"))

      if (dataFiles.length != 1)
        throw new RuntimeException(
          "Unexpected number of files in output directory:\n" + tmpOutDir.toFile
            .listFiles()
            .map(_.getPath)
            .mkString("\n")
        )

      val dataFile = dataFiles.head

      dataFile.renameTo(outPath.toFile)
      tmpOutDir.deleteRecursive()
    }
  }

  implicit class RelationalGroupedDatasetEx(val df: RelationalGroupedDataset) {

    /** Fixes variadic argument passing */
    def aggv(columns: Column*): DataFrame = {
      val head :: tail = columns
      df.agg(head, tail: _*)
    }
  }

}
