package org.cg.monadic.transformer.spark

import org.cg.monadic.transformer.Transformer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Time
import org.apache.spark.sql.Row
import scala.util.Try

/**
 * Spark moving window transformer
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

abstract class DStreamSlideWindowTransformer[IN <: Product: TypeTag] extends Transformer[Unit] {
  def inputStream: DStream[IN]
  def name: String //the name of sliding window and also the name of the in memory table 
  def sql: String // sql to do filter an/or aggregation
  def windowSec: Int
  def slideSec: Int

  override def transform() = {
    import scala.collection.JavaConverters._

    val windowStream: DStream[IN] = inputStream.window(Seconds(windowSec), Seconds(slideSec))
    windowStream.foreachRDD { (rdd: RDD[IN], time: Time) =>
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._
      val df: DataFrame = rdd.toDF()
      df.registerTempTable(name)
      val result = sqlContext.sql(sql)
      val columns = result.columns
      val data = result.collect()
      try {
        onData(name, columns, data)
      } catch {
        case e: Throwable => logger.error(s"Error in handling sliding window $name", e)
      }
    }
  }

  def onData(name: String, columns: Array[String], data: Array[Row]): Unit

}