package KafkaStreaming

class KafkaVariables {
  /** declaring all the required variables */
  val kafkaHost: String = "localhost"
  val kafkaPort: Int = 9092

  val checkpointFileLocation: String = "src/checkpoints/"
  val sampleData: String = "src/main/resources/data/"

  val mysqlHost: String = "jdbc:mysql://localhost:3310/source_db?useServerPrepStmts=false&rewriteBatchedStatements=true"
  val mysqlUsername: String =  "user"
  val mysqlPassword: String = "password"
  val mysqlPort: Int = 3310

  val configDB = "source_db"
  val configTable = "kafka_stream_config"
  val schemaConfigTable = "schema_config"

  val socketHost = "localhost"
  val socketPort = 12345


}
