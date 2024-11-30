package utilities

import KafkaStreaming.KafkaVariables
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import java.util.logging.LogManager


trait ConnectorSpark {

/*
   To return the name of spark application
 */
  def sparkAppName: String

  val spark_conf = new SparkConf()

  protected lazy  val spark: SparkSession =
    SparkSession
      .builder()
      .appName(sparkAppName)
      .master("local[2]")
      .config(spark_conf)
      .config("spark.worker.cleanup.enabled", "true")
      .getOrCreate()

}


trait Helper {
  val kVar =  new KafkaVariables()

  /** for mysql connection */
  lazy val mysql_connection: Connection =
    DriverManager.getConnection(kVar.mysqlHost, kVar.mysqlUsername, kVar.mysqlPassword)

  /** logger */
//  @transient protected lazy val logger: Logger = LogManager.getLogger(getClass)
//  logger.setLevel(Level.ALL)


}
