package nasa.streaming

import nasa.cleaning.StreamingDataCleaning
import nasa.geolocation.IpGeolocation
import nasa.ingestion.DataIngestion
import nasa.transformation.DataTransformation
import nasa.utils.IpUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object StreamingMain {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NASA Log Streaming Analysis")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=" * 60)
    println("  NASA Log Streaming Analysis Pipeline")
    println("=" * 60)

    // ========== 1. CHARGER LES DONNEES GEO (une seule fois) ==========
    println("\n[1/3] Chargement des donnees de geolocalisation...")
    IpUtils.registerUdfs(spark)
    val geoBlocks = DataIngestion.loadGeoBlocks(spark, StreamingConfig.GEO_BLOCKS_PATH)
    val geoLocations = DataIngestion.loadGeoLocations(spark, StreamingConfig.GEO_LOCATIONS_PATH)
    val geoRanges = IpGeolocation.buildGeoRanges(spark, geoBlocks, geoLocations)
    geoRanges.cache()
    println(s"  Plages geo chargees: ${geoRanges.count()} plages")

    // ========== 2. SOURCE KAFKA ==========
    println("\n[2/3] Configuration de la source Kafka...")

    val logSchema = new StructType()
      .add("host", StringType)
      .add("time", LongType)
      .add("method", StringType)
      .add("url", StringType)
      .add("response", IntegerType)
      .add("bytes", LongType)

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", StreamingConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", StreamingConfig.KAFKA_TOPIC)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    // Parser le JSON depuis Kafka
    val parsedStream = kafkaStream
      .selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json(col("json_str"), logSchema).alias("data"))
      .select("data.*")

    // ========== 3. PIPELINE : Clean -> Geolocate -> Transform ==========
    val cleanedStream = StreamingDataCleaning.clean(parsedStream)
    val geolocatedStream = IpGeolocation.joinWithGeo(cleanedStream, geoRanges)
    val enrichedStream = DataTransformation.enrich(geolocatedStream)

    // ========== 4. ANALYSES FENETREES + SINKS ==========
    println("\n[3/3] Demarrage des requetes de streaming...")

    // Analyse 1 : Requetes par pays
    val countryWindowed = StreamingAnalysis.requestsByCountryWindowed(enrichedStream)
    val q1 = countryWindowed.writeStream
      .queryName("requests_by_country")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .foreachBatch(StreamingDatabaseWriter.writeToConsoleAndDb("stream_requests_by_country") _)
      .option("checkpointLocation", s"${StreamingConfig.CHECKPOINT_BASE_DIR}/requests_by_country")
      .start()

    // Analyse 2 : Requetes par categorie URL
    val urlCategoryWindowed = StreamingAnalysis.requestsByUrlCategoryWindowed(enrichedStream)
    val q2 = urlCategoryWindowed.writeStream
      .queryName("requests_by_url_category")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .foreachBatch(StreamingDatabaseWriter.writeToConsoleAndDb("stream_requests_by_url_category") _)
      .option("checkpointLocation", s"${StreamingConfig.CHECKPOINT_BASE_DIR}/requests_by_url_category")
      .start()

    // Analyse 3 : Taux d'erreur
    val errorRateWindowed = StreamingAnalysis.errorRateWindowed(enrichedStream)
    val q3 = errorRateWindowed.writeStream
      .queryName("error_rate")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .foreachBatch(StreamingDatabaseWriter.writeToConsoleAndDb("stream_error_rate") _)
      .option("checkpointLocation", s"${StreamingConfig.CHECKPOINT_BASE_DIR}/error_rate")
      .start()

    // Analyse 4 : Volume de trafic
    val trafficWindowed = StreamingAnalysis.trafficVolumeWindowed(enrichedStream)
    val q4 = trafficWindowed.writeStream
      .queryName("traffic_volume")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .foreachBatch(StreamingDatabaseWriter.writeToConsoleAndDb("stream_traffic_volume") _)
      .option("checkpointLocation", s"${StreamingConfig.CHECKPOINT_BASE_DIR}/traffic_volume")
      .start()

    // Arret propre
    val allQueries = Seq(q1, q2, q3, q4)
    GracefulShutdown.registerHook(allQueries)

    println("  4 requetes de streaming demarrees. En attente de donnees...")
    println("  Appuyez sur Ctrl+C pour arreter.\n")

    spark.streams.awaitAnyTermination()
    spark.stop()
  }
}
