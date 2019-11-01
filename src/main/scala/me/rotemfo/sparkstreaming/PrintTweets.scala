package me.rotemfo.sparkstreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status

/**
 * project: spark-streaming-app
 * package: me.rotemfo.sparkstreaming
 * file:    PrintTweets
 * created: 2019-10-26
 * author:  Rotem
 */
/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets extends BaseTwitterApp {

  override protected def contextWork(): StreamingContext = {

    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = getSparkStreamingContext()

    // Create a DStream from Twitter using our streaming context
    val tweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into RDD's using map()
    val statuses: DStream[String] = tweets.map(_.getText())

    // Print out the first ten
    statuses.print()

    ssc
  }

  def main(args: Array[String]) {
    val context: StreamingContext = contextWork()

    // Kick it all off
    context.start()
    context.awaitTermination()
  }
}