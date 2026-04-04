package nasa.geolocation

import inet.ipaddr.IPAddressString
import nasa.utils.IpUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object IpGeolocation {

  /** Convertit un bloc CIDR en (ip_start, ip_end) */
  private def cidrToStart(cidr: String): Long = {
    if (cidr == null) return -1L
    try {
      val addr = new IPAddressString(cidr).getAddress.toIPv4
      addr.getLower.longValue()
    } catch { case _: Exception => -1L }
  }

  private def cidrToEnd(cidr: String): Long = {
    if (cidr == null) return -1L
    try {
      val addr = new IPAddressString(cidr).getAddress.toIPv4
      addr.getUpper.longValue()
    } catch { case _: Exception => -1L }
  }

  /**
   * Construit la table de reference geo : ip_start, ip_end, country_name, continent_name, etc.
   * Jointure GeoBlocks + GeoLocations sur geoname_id
   */
  def buildGeoRanges(spark: SparkSession, geoBlocks: DataFrame, geoLocations: DataFrame): DataFrame = {
    val cidrToStartUdf = udf(cidrToStart _)
    val cidrToEndUdf = udf(cidrToEnd _)

    val blocksWithRange = geoBlocks
      .withColumn("ip_start", cidrToStartUdf(col("network")))
      .withColumn("ip_end", cidrToEndUdf(col("network")))
      .filter(col("ip_start") >= 0 && col("ip_end") >= 0)
      .select("ip_start", "ip_end", "geoname_id")

    blocksWithRange.join(
      geoLocations.select("geoname_id", "country_name", "continent_name", "country_iso_code", "continent_code"),
      Seq("geoname_id"),
      "inner"
    ).drop("geoname_id")
  }

  /**
   * Joint les logs avec la table geo via range join sur ip_long.
   * Les hosts non-IP gardent des valeurs null pour les colonnes geo.
   */
  def joinWithGeo(logs: DataFrame, geoRanges: DataFrame): DataFrame = {
    // Ajouter ip_long pour les hosts qui sont des adresses IP
    val logsWithIp = logs
      .withColumn("is_ip_host", IpUtils.isIpAddressUdf(col("host")))
      .withColumn("ip_long",
        when(col("is_ip_host"), IpUtils.ipToLongUdf(col("host")))
          .otherwise(lit(-1L).cast(LongType))
      )

    // Separer les logs IP et non-IP
    val ipLogs = logsWithIp.filter(col("is_ip_host") === true)
    val nonIpLogs = logsWithIp.filter(col("is_ip_host") === false)
      .withColumn("country_name", lit("Unknown"))
      .withColumn("continent_name", lit("Unknown"))
      .withColumn("country_iso_code", lit("Unknown"))
      .withColumn("continent_code", lit("Unknown"))

    // Broadcast range join pour les logs IP
    val geolocatedIpLogs = ipLogs.join(
      broadcast(geoRanges),
      ipLogs("ip_long") >= geoRanges("ip_start") && ipLogs("ip_long") <= geoRanges("ip_end"),
      "left"
    ).drop("ip_start", "ip_end")
      // Remplir les nulls pour les IP sans correspondance geo
      .na.fill("Unknown", Seq("country_name", "continent_name", "country_iso_code", "continent_code"))

    // Reunir les deux parties
    nonIpLogs.unionByName(geolocatedIpLogs)
  }
}
