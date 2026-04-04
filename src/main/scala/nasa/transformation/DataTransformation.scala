package nasa.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataTransformation {

  def enrich(df: DataFrame): DataFrame = {
    df
      // Conversion timestamp Unix -> Timestamp Spark
      .withColumn("timestamp", from_unixtime(col("time")).cast("timestamp"))
      // Attributs temporels
      .withColumn("hour_of_day", hour(col("timestamp")))
      .withColumn("day_of_week", date_format(col("timestamp"), "EEEE"))
      .withColumn("date", to_date(col("timestamp")))
      // Categorie URL
      .withColumn("url_category",
        when(col("url").startsWith("/shuttle/"), lit("Shuttle"))
          .when(col("url").startsWith("/history/"), lit("History"))
          .when(col("url").startsWith("/images/"), lit("Images"))
          .when(col("url").startsWith("/facilities/"), lit("Facilities"))
          .when(col("url") === "/" || col("url").startsWith("/ksc"), lit("Homepage"))
          .when(col("url").startsWith("/software/"), lit("Software"))
          .when(col("url").startsWith("/facts/"), lit("Facts"))
          .when(col("url").startsWith("/persons/"), lit("Persons"))
          .when(col("url").startsWith("/elv/"), lit("ELV"))
          .when(col("url").startsWith("/htbin/"), lit("CGI"))
          .when(col("url").startsWith("/cgi-bin/"), lit("CGI"))
          .otherwise(lit("Other"))
      )
      // Categorie code de reponse
      .withColumn("response_category",
        when(col("response").between(200, 299), lit("Success"))
          .when(col("response").between(300, 399), lit("Redirect"))
          .when(col("response").between(400, 499), lit("Client Error"))
          .when(col("response").between(500, 599), lit("Server Error"))
          .otherwise(lit("Other"))
      )
      // Bytes en KB
      .withColumn("bytes_kb", round(col("bytes") / 1024.0, 2))
  }
}
