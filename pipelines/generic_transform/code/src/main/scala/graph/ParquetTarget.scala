package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object ParquetTarget {

  def apply(spark: SparkSession, in: DataFrame): Unit = {
    Config.fabricName match {
      case "demos" =>
        in.write.format("parquet").save(Config.TARGET_PATH)
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
