/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cg.monadic.transformer.csv

import org.cg.monadic.transformer.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.cg.monadic.transformer.Transformer
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import scala.reflect.runtime.universe

/**
 * Set of transformers related to CSV process
 *
 * Usage:
 *     val pipe = for {
 *         csvDF <- CSVToDataFrame(sqlContext, source, Map(("header","true")));
 *         normCsvDF <-TimeDataFormalizer(csvDF, Array("WhenOccurred", "WhenLogged", "LastLogin", "LastSeen", "LastNotify") );
 *         save <- DataFrameToCSV(normCsvDF,target)
 *   } yield (save);
 *   pipe.transform()
 *
 *
 *
 * @author yanlin wang
 */

/**
 * transform CSV -> DataFrame
 */
@deprecated
case class CSVToDataFrame(sqlContext: SQLContext, csvPath: String, options: Map[String, String])
    extends Transformer[DataFrame] {

  override def transform(): DataFrame = {
    val table = sqlContext.read.format("com.databricks.spark.csv").
      options(options)
    if (!options.contains("mode")) table.option("mode", "DROPMALFORMED")

    table.load(csvPath)
  }
}

/**
 *  transform  DataFrame -> CSV
 */
@deprecated
case class DataFrameToCSV(table: DataFrame, csvPath: String)
    extends Transformer[Unit] {

  override def transform() = {
    table.write.format("com.databricks.spark.csv").save(csvPath)
  }
}

/**
 * transform DataFrame -> DataFrame by formalizing date time column from variable
 * length date format 7/1/2015 1:0:0 AM to standard date format
 */
@deprecated
case class TimeDataFormalizer(table: DataFrame, timeCols: Array[String], timeFormat: String = "yyyy-MM-dd HH:mm:ss.SSS", timezone: DateTimeZone = DateTimeZone.UTC)
    extends Transformer[DataFrame] {

  override def transform(): DataFrame = {
    table.registerTempTable("csvtable")
    import org.apache.spark.sql.functions._
    val myFunc = udf { (x: String) => stringToDate(x).toString(timeFormat) }
    val colNames = table.columns
    val cols = colNames.map { x => table.col(x) }
    val mappedCols = cols.map(c => if (timeCols.contains(c.toString())) myFunc(c).as(c.toString()) else c)
    val newTable = table.select(mappedCols: _*)
    newTable
  }

  def stringToDate(ws: String): DateTime = {
    try {
      val sec = ws.split(" ")
      val Array(date, time, am) = sec
      val dates = date.split("/")
      val Array(month, day, year) = dates
      val times = time.split(":")
      val Array(hour, min, second) = times
      val myhour = hour.toInt
      if ("PM".equalsIgnoreCase(am))
        myhour + 12
      val mydate = new DateTime(year.toInt, month.toInt, day.toInt, myhour, min.toInt, second.toInt, 0, timezone)
      mydate
    } catch {
      case e: Exception => {
        logger.info("date time conversion failure", e)
        new DateTime
      }
    }
  }

}