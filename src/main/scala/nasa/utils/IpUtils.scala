package nasa.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object IpUtils {

  private val ipPattern = """^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$""".r

  def isIpAddress(host: String): Boolean = {
    if (host == null) return false
    ipPattern.findFirstIn(host).isDefined
  }

  def ipToLong(ip: String): Long = {
    if (ip == null) return -1L
    try {
      val parts = ip.split("\\.")
      if (parts.length != 4) return -1L
      (parts(0).toLong << 24) + (parts(1).toLong << 16) + (parts(2).toLong << 8) + parts(3).toLong
    } catch {
      case _: NumberFormatException => -1L
    }
  }

  val isIpAddressUdf: UserDefinedFunction = udf(isIpAddress _)
  val ipToLongUdf: UserDefinedFunction = udf(ipToLong _)

  def registerUdfs(spark: SparkSession): Unit = {
    spark.udf.register("is_ip_address", isIpAddress _)
    spark.udf.register("ip_to_long", ipToLong _)
  }
}
