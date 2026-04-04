package nasa.streaming

import java.util.Properties
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import scala.io.Source

object KafkaLogProducer {

  private val errorCount = new AtomicLong(0)

  def main(args: Array[String]): Unit = {
    val delayMs = if (args.length > 0) args(0).toLong else 10L
    val csvPath = if (args.length > 1) args(1) else "data/data.csv"

    val props = new Properties()
    props.put("bootstrap.servers", StreamingConfig.KAFKA_BOOTSTRAP_SERVERS)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("acks", "all")
    props.put("retries", "3")
    props.put("retry.backoff.ms", "1000")

    val producer = new KafkaProducer[String, String](props)

    val callback = new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          val errors = errorCount.incrementAndGet()
          if (errors % 100 == 1) {
            println(s"  [ERROR] Send failed ($errors total): ${exception.getMessage}")
          }
        }
      }
    }

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
          producer.send(record, callback)

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

    val errors = errorCount.get()
    println(s"\nDone. Total records sent: $count (errors: $errors)")
  }

  private def escape(s: String): String =
    s.replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
}
