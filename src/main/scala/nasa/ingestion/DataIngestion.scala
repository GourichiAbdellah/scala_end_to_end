package nasa.ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataIngestion {

  def loadNasaLogs(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(path)
  }

  def loadGeoBlocks(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  def loadGeoLocations(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }
}
