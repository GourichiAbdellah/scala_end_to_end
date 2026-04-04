package nasa.cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}

object DataCleaning {

  def clean(df: DataFrame): DataFrame = {
    // Identifier la colonne index (premiere colonne sans nom ou nommee "_c0")
    val indexCol = df.columns.head

    df
      // Supprimer la colonne index
      .drop(indexCol)
      // Trim whitespace sur les colonnes texte
      .withColumn("host", trim(col("host")))
      .withColumn("method", trim(col("method")))
      .withColumn("url", trim(col("url")))
      // Cast des types
      .withColumn("time", col("time").cast(LongType))
      .withColumn("response", col("response").cast(IntegerType))
      .withColumn("bytes", col("bytes").cast(LongType))
      // Filtrer les lignes malformees (response null apres cast = non numerique)
      .filter(col("response").isNotNull)
      // Filtrer les lignes sans host ou url
      .filter(col("host").isNotNull && col("host") =!= "")
      .filter(col("url").isNotNull && col("url") =!= "")
      // Remplacer bytes null par 0
      .na.fill(0L, Seq("bytes"))
  }
}
