package nasa.cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}

object StreamingDataCleaning {

  def clean(df: DataFrame): DataFrame = {
    df
      // Trim whitespace sur les colonnes texte
      .withColumn("host", trim(col("host")))
      .withColumn("method", trim(col("method")))
      .withColumn("url", trim(col("url")))
      // Cast des types (JSON peut inferer des strings)
      .withColumn("time", col("time").cast(LongType))
      .withColumn("response", col("response").cast(IntegerType))
      .withColumn("bytes", col("bytes").cast(LongType))
      // Filtrer les lignes malformees
      .filter(col("response").isNotNull)
      .filter(col("host").isNotNull && col("host") =!= "")
      .filter(col("url").isNotNull && col("url") =!= "")
      // Remplacer bytes null par 0
      .na.fill(0L, Seq("bytes"))
  }
}
