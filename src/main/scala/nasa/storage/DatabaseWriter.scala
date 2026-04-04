package nasa.storage

import org.apache.spark.sql.DataFrame
import java.util.Properties

object DatabaseWriter {

  private val url = sys.env.getOrElse("POSTGRES_URL", "jdbc:postgresql://localhost:5432/nasa_pipeline")

  private def connectionProperties(): Properties = {
    val props = new Properties()
    props.setProperty("user", sys.env.getOrElse("POSTGRES_USER", "spark_user"))
    props.setProperty("password", sys.env.getOrElse("POSTGRES_PASSWORD", "motdepasse"))
    props.setProperty("driver", "org.postgresql.Driver")
    props
  }

  /** Ecrit un DataFrame dans une table PostgreSQL */
  def writeToDb(df: DataFrame, tableName: String): Unit = {
    df.write
      .mode("overwrite")
      .jdbc(url, tableName, connectionProperties())
  }

  /** Ecrit le DataFrame principal avec options de performance */
  def writeLargeToDb(df: DataFrame, tableName: String): Unit = {
    df.write
      .mode("overwrite")
      .option("batchsize", 10000)
      .option("numPartitions", 8)
      .jdbc(url, tableName, connectionProperties())
  }

  /** Exporte un DataFrame en CSV (un seul fichier) */
  def exportToCsv(df: DataFrame, outputPath: String): Unit = {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)
  }
}
