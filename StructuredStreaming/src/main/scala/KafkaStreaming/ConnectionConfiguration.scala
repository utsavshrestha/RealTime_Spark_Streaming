package KafkaStreaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import utilities.{ConnectorSpark, Helper}

class ConnectionConfiguration extends Helper with ConnectorSpark{

  override def sparkAppName: String = "All required connection"


  /**
   * this function is used to read the mysql table
   * @param tableName: name of the name
   * @param partitionColumn: column name that will be used as partition
   * @return
   */
  def readMySQL(
                 tableName: String
               ) = {
    spark.read.format("jdbc")
      .option("url", kVar.mysqlHost)
      .option("user", kVar.mysqlUsername)
      .option("password", kVar.mysqlPassword)
      .option("dbtable", tableName)
      .load()
  }

  /**
   * This function is used to read from the kafka source
   * @param topicName: we need to provide the name of the topic that we want to fetch
   * from the kafka server
   * topicName: we can have multiple topic name to read from the server at the same time
   * we need to add the topic name using , for eg subscribe topicName1, topicName2, topicName3
   * @return
   */
  def readKafkaSource(topicName: String, startingOffSets: String): DataFrame =
    try {
      println(s"Starting stream for ${topicName}")
      spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"${kVar.kafkaHost}:${kVar.kafkaPort}")
      .option("subscribe", topicName)
      .option("startingOffsets", startingOffSets)
      .load()

    }
    catch {
      case e: Exception =>
        println(s"Failed to start streaming for topic $topicName" + e.getMessage, e)
        throw  e
    }

  def writeKafkaStreamToParquet(
                                 df: DataFrame,
                                 outputPath: String,
                                 checkpointPath: String,
                                 triggerInterval: String,
                                 outputMode: String
                               ): StreamingQuery =
    df
      .writeStream
      .format("console")
      .option("path", outputPath)
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .outputMode(outputMode)
      .start()


}
