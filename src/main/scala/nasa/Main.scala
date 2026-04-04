package nasa

import nasa.analysis.LogAnalysis
import nasa.cleaning.DataCleaning
import nasa.geolocation.IpGeolocation
import nasa.ingestion.DataIngestion
import nasa.storage.DatabaseWriter
import nasa.transformation.DataTransformation
import nasa.utils.IpUtils
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NASA Log Analysis")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=" * 60)
    println("  NASA Log Analysis Pipeline")
    println("=" * 60)

    // ========== 1. INGESTION ==========
    println("\n[1/6] Ingestion des donnees...")
    val rawLogs = DataIngestion.loadNasaLogs(spark, "data/data.csv")
    val geoBlocks = DataIngestion.loadGeoBlocks(spark, "data/GeoLite2-Country-Blocks-IPv4.csv")
    val geoLocations = DataIngestion.loadGeoLocations(spark, "data/GeoLite2-Country-Locations-en.csv")
    println(s"  Logs bruts: ${rawLogs.count()} lignes")
    println(s"  Blocs geo: ${geoBlocks.count()} lignes")
    println(s"  Locations geo: ${geoLocations.count()} lignes")

    // ========== 2. NETTOYAGE ==========
    println("\n[2/6] Nettoyage des donnees...")
    val cleanedLogs = DataCleaning.clean(rawLogs)
    println(s"  Logs apres nettoyage: ${cleanedLogs.count()} lignes")

    // ========== 3. GEOLOCALISATION ==========
    println("\n[3/6] Geolocalisation des adresses IP...")
    IpUtils.registerUdfs(spark)
    val geoRanges = IpGeolocation.buildGeoRanges(spark, geoBlocks, geoLocations)
    println(s"  Plages geo construites: ${geoRanges.count()} plages")
    val geolocatedLogs = IpGeolocation.joinWithGeo(cleanedLogs, geoRanges)

    // ========== 4. TRANSFORMATION ==========
    println("\n[4/6] Transformation et enrichissement...")
    val enrichedLogs = DataTransformation.enrich(geolocatedLogs)

    // Cache pour les analyses
    enrichedLogs.cache()
    val totalRows = enrichedLogs.count()
    println(s"  Logs enrichis: $totalRows lignes")
    println("\n  Schema final:")
    enrichedLogs.printSchema()

    // ========== 5. ANALYSES ==========
    println("\n[5/6] Execution des 15 analyses...")

    val analyses: Map[String, org.apache.spark.sql.DataFrame] = Map(
      "traffic_by_country"        -> LogAnalysis.trafficByCountry(enrichedLogs),
      "traffic_by_continent"      -> LogAnalysis.trafficByContinent(enrichedLogs),
      "top_pages"                 -> LogAnalysis.topPages(enrichedLogs),
      "error_analysis"            -> LogAnalysis.errorAnalysis(enrichedLogs),
      "hourly_traffic"            -> LogAnalysis.hourlyTraffic(enrichedLogs),
      "daily_traffic"             -> LogAnalysis.dailyTraffic(enrichedLogs),
      "day_of_week_traffic"       -> LogAnalysis.dayOfWeekTraffic(enrichedLogs),
      "bandwidth_by_category"     -> LogAnalysis.bandwidthByCategory(enrichedLogs),
      "top_hosts"                 -> LogAnalysis.topHosts(enrichedLogs),
      "url_category_distribution" -> LogAnalysis.urlCategoryDistribution(enrichedLogs),
      "response_distribution"     -> LogAnalysis.responseDistribution(enrichedLogs),
      "country_hour_heatmap"      -> LogAnalysis.countryHourHeatmap(enrichedLogs),
      "top_error_pages"           -> LogAnalysis.topErrorPages(enrichedLogs),
      "daily_bandwidth"           -> LogAnalysis.dailyBandwidth(enrichedLogs),
      "method_distribution"       -> LogAnalysis.methodDistribution(enrichedLogs)
    )

    // Afficher un apercu de chaque analyse
    analyses.foreach { case (name, df) =>
      println(s"\n  --- $name ---")
      df.show(5, truncate = false)
    }

    // ========== 6. STOCKAGE ==========
    println("\n[6/6] Stockage des resultats...")

    // Export CSV
    println("  Export CSV dans output/...")
    analyses.foreach { case (name, df) =>
      DatabaseWriter.exportToCsv(df, s"output/$name")
    }
    // Export du dataset enrichi en Parquet
    enrichedLogs.write.mode("overwrite").parquet("output/enriched_logs_parquet")
    println("  Dataset enrichi exporte en Parquet: output/enriched_logs_parquet")

    // Stockage PostgreSQL (commenter si PostgreSQL n'est pas disponible)
    try {
      println("  Ecriture dans PostgreSQL...")
      DatabaseWriter.writeLargeToDb(enrichedLogs, "cleaned_logs")
      analyses.foreach { case (name, df) =>
        DatabaseWriter.writeToDb(df, name)
      }
      println("  PostgreSQL: OK")
    } catch {
      case e: Exception =>
        println(s"  PostgreSQL non disponible: ${e.getMessage}")
        println("  Les resultats CSV et Parquet sont toujours disponibles dans output/")
    }

    println("\n" + "=" * 60)
    println("  Pipeline termine avec succes!")
    println("=" * 60)

    spark.stop()
  }
}
