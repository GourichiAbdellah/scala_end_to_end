package nasa.streaming

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import scala.io.Source

object KafkaLogProducer {

  def main(args: Array[String]): Unit = {
    val delayMs = if (args.length > 0) args(0).toLong else 10L
    val csvPath = if (args.length > 1) args(1) else "data/data.csv"

    val props = new Properties()
    props.put("bootstrap.servers", StreamingConfig.KAFKA_BOOTSTRAP_SERVERS)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("acks", "1")

    val producer = new KafkaProducer[String, String](props)

    println("=" * 60)
    println("  NASA Log Kafka Producer")
    println(s"  Topic: ${StreamingConfig.KAFKA_TOPIC}")
    println(s"  Delay: ${delayMs}ms between messages")
    println("=" * 60)

    val source = Source.fromFile(csvPath)
    val lines = source.getLines()
    lines.next() // skip header

    var count = 0L
    try {
      for (line <- lines) {
        val parts = line.split(",", 7)
        if (parts.length >= 7) {
          val host = parts(1).replace("\"", "")
          val time = parts(2)
          val method = parts(3)
          val url = parts(4)
          val response = parts(5)
          val bytes = if (parts(6).isEmpty) "0" else parts(6)

          val json = s"""{"host":"${escape(host)}","time":$time,"method":"${escape(method)}","url":"${escape(url)}","response":$response,"bytes":$bytes}"""

          val record = new ProducerRecord[String, String](
            StreamingConfig.KAFKA_TOPIC, host, json
          )
          producer.send(record)

          count += 1
          if (count % 10000 == 0) println(s"  Sent $count records...")
          if (delayMs > 0) Thread.sleep(delayMs)
        }
      }
    } finally {
      producer.flush()
      producer.close()
      source.close()
    }

    println(s"\nDone. Total records sent: $count")
  }

  private def escape(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"")
}
