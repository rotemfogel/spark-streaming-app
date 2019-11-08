package me.rotemfo.sparkstreaming.twitter

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

/** Listens to a stream of tweets and saves them to disk. */
object SaveTweets extends BaseTwitterApp {

  override protected def contextWork(): StreamingContext = {
    val ssc = getSparkStreamingContext(duration = Seconds(1))

    // Create a DStream from Twitter using our streaming context
    val tweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText)

    // Keep count of how many Tweets we've received so we can stop automatically
    // (and not fill up your disk!)
    var totalTweets: Long = 0

    statuses.foreachRDD((rdd, time) => {
      // Don't bother with empty batches
      if (rdd.count() > 0) {
        // Combine each partition's results into a single RDD:
        val repartitionedRDD = rdd.coalesce(1).cache()
        // And print out a directory with the results.
        repartitionedRDD.saveAsTextFile("output/tweets_" + time.milliseconds.toString)
        // Stop once we've collected 1000 tweets.
        totalTweets += repartitionedRDD.count()
        logger.info(s"Tweet count: $totalTweets")
        if (totalTweets > 1000) {
          System.exit(0)
        }
      }
    })
    ssc
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    val context: StreamingContext = contextWork()

    // Kick it all off
    context.start()
    context.awaitTermination()
  }
}
