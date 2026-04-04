package nasa.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object StreamingAnalysis {

  /** Requetes par pays dans des fenetres de 5 minutes */
  def requestsByCountryWindowed(df: DataFrame): DataFrame = {
    df.withWatermark("timestamp", StreamingConfig.WATERMARK_DURATION)
      .filter(col("country_name") =!= "Unknown")
      .groupBy(
        window(col("timestamp"), StreamingConfig.WINDOW_DURATION),
        col("country_name")
      )
      .agg(
        count("*").alias("requests"),
        sum("bytes").alias("total_bytes")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("country_name"),
        col("requests"),
        col("total_bytes")
      )
  }

  /** Requetes par categorie URL dans des fenetres de 5 minutes */
  def requestsByUrlCategoryWindowed(df: DataFrame): DataFrame = {
    df.withWatermark("timestamp", StreamingConfig.WATERMARK_DURATION)
      .groupBy(
        window(col("timestamp"), StreamingConfig.WINDOW_DURATION),
        col("url_category")
      )
      .agg(
        count("*").alias("requests"),
        sum("bytes").alias("total_bytes"),
        round(avg("bytes"), 2).alias("avg_bytes")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("url_category"),
        col("requests"),
        col("total_bytes"),
        col("avg_bytes")
      )
  }

  /** Taux d'erreur dans des fenetres de 5 minutes */
  def errorRateWindowed(df: DataFrame): DataFrame = {
    df.withWatermark("timestamp", StreamingConfig.WATERMARK_DURATION)
      .groupBy(
        window(col("timestamp"), StreamingConfig.WINDOW_DURATION)
      )
      .agg(
        count("*").alias("total_requests"),
        sum(when(col("response") >= 400, 1).otherwise(0)).alias("error_requests"),
        round(
          sum(when(col("response") >= 400, 1).otherwise(0)).cast("double") /
            count("*").cast("double") * 100, 2
        ).alias("error_rate_pct")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_requests"),
        col("error_requests"),
        col("error_rate_pct")
      )
  }

  /** Volume de trafic dans des fenetres de 5 minutes */
  def trafficVolumeWindowed(df: DataFrame): DataFrame = {
    df.withWatermark("timestamp", StreamingConfig.WATERMARK_DURATION)
      .groupBy(
        window(col("timestamp"), StreamingConfig.WINDOW_DURATION)
      )
      .agg(
        count("*").alias("requests"),
        sum("bytes").alias("total_bytes"),
        round(sum("bytes") / 1024.0 / 1024.0, 2).alias("total_mb"),
        approx_count_distinct("host").alias("unique_hosts")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("requests"),
        col("total_bytes"),
        col("total_mb"),
        col("unique_hosts")
      )
  }
}
