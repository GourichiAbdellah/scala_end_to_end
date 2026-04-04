package nasa.streaming

object StreamingConfig {
  val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
  val KAFKA_TOPIC = "nasa-logs"
  val WINDOW_DURATION = "5 minutes"
  val WATERMARK_DURATION = "10 minutes"
  val CHECKPOINT_BASE_DIR = "checkpoints"
  val GEO_BLOCKS_PATH = "data/GeoLite2-Country-Blocks-IPv4.csv"
  val GEO_LOCATIONS_PATH = "data/GeoLite2-Country-Locations-en.csv"
}
