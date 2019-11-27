package me.rotemfo.sparkstreaming.twitter

import me.rotemfo.common.Logging
import me.rotemfo.sparkstreaming.Utilities.setupTwitter
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * project: spark-streaming-app
 * package: me.rotemfo.sparkstreaming
 * file:    BaseTwitterApp
 * created: 2019-10-27
 * author:  rotem
 */
trait BaseTwitterApp extends Logging {
  protected final val appName: String = getClass.getSimpleName.replaceAll("\\$", "")
  protected final val checkpointDefaultDir: String = s"checkpoint/$appName"

  // Configure Twitter credentials
  setupTwitter()

  protected def getSparkStreamingContext(master: String = "local[*]", appName: String = appName, duration: Duration = Seconds(1)): StreamingContext = {
    new StreamingContext(master, appName, duration)
  }

  protected def contextWork(): StreamingContext
}
