package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object ParseConfig {
  def apply(spark: SparkSession): DataFrame = {
    
    val lengthValidationType = ArrayType(new StructType()
      .add("column_name", StringType)
      .add("type_cast", StringType)
      .add("lengths", ArrayType(IntegerType))
    )
    
    val dateValidationType = ArrayType(new StructType()
      .add("column_name", StringType)
      .add("current_format", StringType)
      .add("new_format", StringType)
    )
    
    val formatValidationType = ArrayType(new StructType()
      .add("column_name", StringType)
      .add("format", StringType)
    )
    
    val json_schema = StructType(
              Array(
                StructField("length_validations", lengthValidationType, true),
                StructField("date_conversions", dateValidationType, true),
                StructField("format_validations", formatValidationType, true),
              )
            )
    
    val df = Seq(Config.config).toDF("json")
    val out0 = df.withColumn("config", from_json(col("json"),json_schema))
    out0
  }

}
