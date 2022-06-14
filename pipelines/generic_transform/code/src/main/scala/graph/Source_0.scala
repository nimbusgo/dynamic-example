package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object Source_0 {

  def apply(spark: SparkSession): DataFrame = {
    Config.fabricName match {
      case "demos" =>
        spark.read
          .format("csv")
          .option("header",      true)
          .option("inferSchema", true)
          .option("sep",         ",")
          .load(Config.SOURCE_PATH)
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
