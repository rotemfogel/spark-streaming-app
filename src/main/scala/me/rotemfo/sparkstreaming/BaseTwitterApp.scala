package me.rotemfo.sparkstreaming

import me.rotemfo.sparkstreaming.Utilities.setupTwitter
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * project: spark-streaming-app
 * package: me.rotemfo.sparkstreaming
 * file:    BaseTwitterApp
 * created: 2019-10-27
 * author:  rotem
 */
trait BaseTwitterApp extends Logging {
  // Configure Twitter credentials using twitter.txt
  setupTwitter()

  protected def getSparkStreamingContext(master: String = "local[*]", appName: String = getClass.getSimpleName, duration: Duration = Seconds(1)): StreamingContext = {
    new StreamingContext(master, appName, duration)
  }
}
