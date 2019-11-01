package me.rotemfo.sparkstreaming

import me.rotemfo.sparkstreaming.Utilities.setupTwitter
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * project: spark-streaming-app
 * package: me.rotemfo.sparkstreaming
 * file:    BaseTwitterApp
 * created: 2019-10-27
 * author:  rotem
 */
trait BaseTwitterApp {
  protected final val logger: Logger = LoggerFactory.getLogger(getClass)
  private final val className: String = getClass.getSimpleName.replaceAll("\\$", "")
  private final val checkpointDefaultDir: String = s"checkpoint/$className"
  private var ssc: StreamingContext = _
  // Configure Twitter credentials
  setupTwitter()

  //noinspection ScalaUnusedSymbol
  private def createStreamingContext(checkpointDir: String, master: String, appName: String, duration: Duration): StreamingContext = {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val ssc = new StreamingContext(conf, duration)
    ssc.checkpoint(checkpointDir)
    ssc
  }

  protected def getSparkStreamingContext(checkpointDir: String = checkpointDefaultDir, master: String = "local[*]", appName: String = className, duration: Duration = Seconds(1)): StreamingContext = {
    // TODO: check why getOrCreate not working from checkpoint dir
    // ssc = StreamingContext.getOrCreate(checkpointDir, () => createStreamingContext(checkpointDir, master, appName, duration))
    ssc = new StreamingContext(master, appName, duration)
    ssc.checkpoint(checkpointDir)
    ssc
  }

  sys.addShutdownHook(() => {
    logger.info("spark context stopping")
    ssc.stop()
  })
}
