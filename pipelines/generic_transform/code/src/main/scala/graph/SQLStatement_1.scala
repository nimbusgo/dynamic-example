package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object SQLStatement_1 {

  def apply(spark: SparkSession, source_data: DataFrame): DataFrame = {
    source_data.createOrReplaceTempView("source_data")
    spark.sql(Config.TRANSFORM_SQL)
  }

}
