package nasa.streaming

import org.apache.spark.sql.streaming.StreamingQuery

object GracefulShutdown {

  def registerHook(queries: Seq[StreamingQuery]): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread("streaming-shutdown-hook") {
      override def run(): Unit = {
        println("\n  Shutdown signal received. Stopping streaming queries...")
        queries.foreach { q =>
          println(s"  Stopping query: ${q.name}")
          q.stop()
        }
        println("  All streaming queries stopped.")
      }
    })
  }
}
