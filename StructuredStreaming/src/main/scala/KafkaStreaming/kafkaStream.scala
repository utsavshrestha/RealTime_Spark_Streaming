package KafkaStreaming

/** imports */
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

/** need to experiment with future and ExecutionContext to run parallel and in seperate threads */
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/** import helper packages */
import utilities.{ConnectorSpark, Helper}

object kafkaStream extends ConnectionConfiguration with ConnectorSpark with Helper {


  def getSchema(schemaJson: String): StructType = {
    DataType.fromJson(schemaJson).asInstanceOf[StructType]
  }

  def startKafkaStream(row: Row): (StreamingQuery, Int) = {
    val topicName = row.getString(row.fieldIndex("topic_name"))
    val startingOffSets = row.getString(row.fieldIndex("starting_offsets"))
    val schemaJson = row.getString(row.fieldIndex("topicSchema"))
    val outputPath = row.getString(row.fieldIndex("output_path"))
    val checkpointPath = row.getString(row.fieldIndex("checkpoint_path"))
    val triggerInterval = Option(row.getString(row.fieldIndex("trigger_interval")))
    val outputMode = row.getString(row.fieldIndex("output_mode"))
    val awaitTermiationDuratrion = row.getAs[Int]("await_termination_duration")
    val triggerIntervalStr = triggerInterval.getOrElse("10 seconds")
    /** fetching the defined schema types */
    val schema = getSchema(schemaJson)
    /** reading from kafka stream
     * and converting encrypted data to readable format
     * and then converting the string value to required data type
      */
    val kafkaStream =
      readKafkaSource(topicName, startingOffSets)
      .selectExpr("CAST(value as STRING)")
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")
    /** now writing to parque file */
    val streamingQuery =
      writeKafkaStreamToParquet(kafkaStream, outputPath, checkpointPath,triggerIntervalStr, outputMode)
    (streamingQuery, awaitTermiationDuratrion)

  }

 def startStreaming() = {
   try {

     /** fetching config table */
     val configDB: DataFrame = readMySQL(
       s"${kVar.configDB}.${kVar.configTable}")
     configDB.show()
//       .where(col("active_flag") === 1)

     /** fetching scheme config table */
     val schemaDB: DataFrame = readMySQL(
       s"${kVar.configDB}.${kVar.schemaConfigTable}")
     schemaDB.show()

     val joinConfigDB = configDB
       .join(schemaDB,
         configDB("schema_id") === schemaDB("schema_id"),
         "inner")
       .select(
         col("topic_name"),
         col("starting_offsets"),
         col("topicSchema"),
         col("output_path"),
         col("checkpoint_path"),
         col("trigger_interval"),
         col("output_mode"),
         col("await_termination_duration")
       )
      joinConfigDB.show()
     /** streaming query in parallel */
     val queryHandle = joinConfigDB.collect().flatMap { row =>
       try {
         println("Starting kafka stream in parallel")
         Some(startKafkaStream(row))
       }
       catch {
         case e: Exception =>
           println(s"Failed to start stream for topic " +
             s"${row.getString(row.fieldIndex("topic_name"))}: ${e.getMessage}",
             e)
           None
       }
     }

     /** waiting for all the streaming to end as per their awaitTermination Configuration */
     queryHandle.foreach { case (query, awaitTerminationDuration) =>
       if (awaitTerminationDuration > 0) {
         println(s"Stream will terminate after ${awaitTerminationDuration} seconds")
         query.awaitTermination(awaitTerminationDuration * 1000L)

         /** converting second into millisecond as awaitTermination func is based on millisecond run */
       }
       else {
         println("Stream will run indefinitely")
         query.awaitTermination()
       }

     }

   }
   catch {
     case e: Exception =>
       println("Error occured during the streaming job run" + e.getMessage, e)
       throw e

   }
 }



  def main(args: Array[String]): Unit = {
    startStreaming()
}
}