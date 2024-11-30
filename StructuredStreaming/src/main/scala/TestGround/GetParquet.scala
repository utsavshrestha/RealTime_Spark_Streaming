package TestGround

import KafkaStreaming.ConnectionConfiguration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import utilities.{ConnectorSpark, Helper}

object GetParquet extends ConnectionConfiguration with ConnectorSpark with Helper{

  def main(args: Array[String]): Unit = {

    val voterSchema = StructType(Array(
      StructField("voter_id", StringType, nullable = false),
      StructField("voter_name", StringType, nullable = true),
      StructField("date_of_birth", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("nationality", StringType, nullable = true),
      StructField("registration_number", StringType, nullable = true),
      StructField("address_street", StringType, nullable = true),
      StructField("address_city", StringType, nullable = true),
      StructField("address_state", StringType, nullable = true),
      StructField("address_country", StringType, nullable = true),
      StructField("address_postcode", StringType, nullable = true),
      StructField("email", StringType, nullable = true),
      StructField("phone_number", StringType, nullable = true),
      StructField("cell_number", StringType, nullable = true),
      StructField("picture", StringType, nullable = true),
      StructField("registered_age", IntegerType, nullable = true)
    ))
     val check = spark.read.parquet("src/parquetLocation/voter")
       .withColumn("registrationAge", col("registered_age").cast("int"))
       .groupBy(col("registrationAge"))
       .agg(count("*").alias("total_age_group"))

    check.show(false)


  }

}
