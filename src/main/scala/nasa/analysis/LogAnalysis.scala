package nasa.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object LogAnalysis {

  // 1. Trafic par pays (top 20)
  def trafficByCountry(df: DataFrame): DataFrame = {
    df.filter(col("country_name") =!= "Unknown")
      .groupBy("country_name", "country_iso_code")
      .agg(
        count("*").alias("requests"),
        sum("bytes").alias("total_bytes"),
        countDistinct("host").alias("unique_hosts")
      )
      .orderBy(desc("requests"))
      .limit(20)
  }

  // 2. Trafic par continent
  def trafficByContinent(df: DataFrame): DataFrame = {
    df.filter(col("continent_name") =!= "Unknown")
      .groupBy("continent_name", "continent_code")
      .agg(
        count("*").alias("requests"),
        sum("bytes").alias("total_bytes"),
        countDistinct("host").alias("unique_hosts")
      )
      .orderBy(desc("requests"))
  }

  // 3. Top 50 pages les plus demandees
  def topPages(df: DataFrame): DataFrame = {
    df.groupBy("url")
      .agg(
        count("*").alias("requests"),
        sum("bytes").alias("total_bytes")
      )
      .orderBy(desc("requests"))
      .limit(50)
  }

  // 4. Analyse des erreurs (distribution codes + pourcentage)
  def errorAnalysis(df: DataFrame): DataFrame = {
    val total = df.count().toDouble
    df.groupBy("response", "response_category")
      .agg(count("*").alias("requests"))
      .withColumn("percentage", round(col("requests") / lit(total) * 100, 2))
      .orderBy(desc("requests"))
  }

  // 5. Trafic par heure (pattern horaire)
  def hourlyTraffic(df: DataFrame): DataFrame = {
    df.groupBy("hour_of_day")
      .agg(
        count("*").alias("requests"),
        sum("bytes").alias("total_bytes")
      )
      .orderBy("hour_of_day")
  }

  // 6. Trafic journalier (evolution dans le temps)
  def dailyTraffic(df: DataFrame): DataFrame = {
    df.groupBy("date")
      .agg(
        count("*").alias("requests"),
        countDistinct("host").alias("unique_hosts")
      )
      .orderBy("date")
  }

  // 7. Trafic par jour de la semaine
  def dayOfWeekTraffic(df: DataFrame): DataFrame = {
    df.groupBy("day_of_week")
      .agg(
        count("*").alias("requests"),
        sum("bytes").alias("total_bytes")
      )
      .orderBy(desc("requests"))
  }

  // 8. Bande passante par categorie URL
  def bandwidthByCategory(df: DataFrame): DataFrame = {
    df.groupBy("url_category")
      .agg(
        count("*").alias("requests"),
        sum("bytes").alias("total_bytes"),
        round(avg("bytes"), 2).alias("avg_bytes")
      )
      .orderBy(desc("total_bytes"))
  }

  // 9. Top 50 hosts les plus actifs
  def topHosts(df: DataFrame): DataFrame = {
    df.groupBy("host")
      .agg(
        count("*").alias("requests"),
        sum("bytes").alias("total_bytes")
      )
      .orderBy(desc("requests"))
      .limit(50)
  }

  // 10. Distribution des categories URL
  def urlCategoryDistribution(df: DataFrame): DataFrame = {
    val total = df.count().toDouble
    df.groupBy("url_category")
      .agg(count("*").alias("requests"))
      .withColumn("percentage", round(col("requests") / lit(total) * 100, 2))
      .orderBy(desc("requests"))
  }

  // 11. Distribution des codes de reponse
  def responseDistribution(df: DataFrame): DataFrame = {
    df.groupBy("response", "response_category")
      .agg(count("*").alias("requests"))
      .orderBy(desc("requests"))
  }

  // 12. Heatmap pays x heure (top 10 pays)
  def countryHourHeatmap(df: DataFrame): DataFrame = {
    val topCountries = df
      .filter(col("country_name") =!= "Unknown")
      .groupBy("country_name")
      .agg(count("*").alias("total"))
      .orderBy(desc("total"))
      .limit(10)
      .select("country_name")

    df.join(topCountries, Seq("country_name"), "inner")
      .groupBy("country_name", "hour_of_day")
      .agg(count("*").alias("requests"))
      .orderBy("country_name", "hour_of_day")
  }

  // 13. Top 30 pages en erreur 404
  def topErrorPages(df: DataFrame): DataFrame = {
    df.filter(col("response") === 404)
      .groupBy("url")
      .agg(count("*").alias("requests"))
      .orderBy(desc("requests"))
      .limit(30)
  }

  // 14. Bande passante journaliere
  def dailyBandwidth(df: DataFrame): DataFrame = {
    df.groupBy("date")
      .agg(
        sum("bytes").alias("total_bytes"),
        round(sum("bytes") / 1024.0 / 1024.0, 2).alias("total_mb")
      )
      .orderBy("date")
  }

  // 15. Distribution des methodes HTTP
  def methodDistribution(df: DataFrame): DataFrame = {
    val total = df.count().toDouble
    df.groupBy("method")
      .agg(count("*").alias("requests"))
      .withColumn("percentage", round(col("requests") / lit(total) * 100, 2))
      .orderBy(desc("requests"))
  }
}
