package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object Validate {
  def apply(spark: SparkSession, input_data: DataFrame, configDF: DataFrame): DataFrame = {
    import spark.implicits._
    import scala.collection.mutable.WrappedArray
    
    val typeMappings = Map(
        "StringType" -> StringType,
        "DecimalType" -> DecimalType
    )
    
    val config = configDF.select("config").collect().map(_.getAs[Row]("config"))
    
    val length_validations = config.flatMap(_.getAs[WrappedArray[Row]]("length_validations")).map{
        case Row(column_name: String, type_cast: String, lengths: WrappedArray[Int]) => {
            val format: Option[Any] = Some(typeMappings.getOrElse(type_cast, type_cast))
            is_valid(col(column_name),formatInfo=format,len=Some(lengths.toSeq))
        }
        case _ => lit(true)
    }
    
    val date_conversions = config.flatMap(_.getAs[WrappedArray[Row]]("date_conversions")).map{
        case Row(column_name: String, current_format: String, new_format: String) => {
            val format = Some(List(current_format, new_format))
            is_valid(col(column_name),formatInfo= format)
        }
        case _ => lit(true)
    }
    
    val format_validations = config.flatMap(_.getAs[WrappedArray[Row]]("format_validations")).map{
        case Row(column_name: String, format: String) => {
            is_valid(col(column_name),formatInfo= Some(typeMappings.getOrElse(format, format)))
        }
        case _ => lit(true)
    }
    
    val allValid = ((length_validations ++ date_conversions ++ format_validations) :+ lit(true)).reduce((exp, c) => exp.and(c))
    
    val out0 = input_data.filter(allValid)
    out0
  }

}
