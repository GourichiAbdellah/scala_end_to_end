package nasa.streaming

import org.apache.spark.sql.DataFrame
import java.util.Properties

object StreamingDatabaseWriter {

  private val url = "jdbc:postgresql://localhost:5432/nasa_pipeline"

  private def connectionProperties(): Properties = {
    val props = new Properties()
    props.setProperty("user", "spark_user")
    props.setProperty("password", "motdepasse")
    props.setProperty("driver", "org.postgresql.Driver")
    props
  }

  /** Ecrit un micro-batch dans la console et dans PostgreSQL */
  def writeToConsoleAndDb(tableName: String)(batchDf: DataFrame, batchId: Long): Unit = {
    if (!batchDf.isEmpty) {
      println(s"\n  === [Batch $batchId] $tableName ===")
      batchDf.show(20, truncate = false)

      try {
        batchDf.write
          .mode("append")
          .jdbc(url, tableName, connectionProperties())
      } catch {
        case e: Exception =>
          println(s"  PostgreSQL write failed for $tableName: ${e.getMessage}")
      }
    }
  }
}
