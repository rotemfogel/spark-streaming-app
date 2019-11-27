package me.rotemfo.sparkstreaming.twitter

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Uses thread-safe counters to keep track of the average length of
 * Tweets in a stream.
 */
object AverageTweetLength extends BaseTwitterApp {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val context = contextWork()
    context.start()
    context.awaitTermination()
  }

  override protected def contextWork(): StreamingContext = {
    // Set up a Spark streaming context named "AverageTweetLength" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "AverageTweetLength", Seconds(1))

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText)

    // Map this to tweet character lengths.
    val lengths = statuses.map(status => status.length)

    // As we could have multiple processes adding into these running totals
    // at the same time, we'll just Java's AtomicLong class to make sure
    // these counters are thread-safe.
    val totalTweets = new AtomicLong(0)
    var totalChars = new AtomicLong(0)

    // In Spark 1.6+, you  might also look into the mapWithState function, which allows
    // you to safely and efficiently keep track of global state with key/value pairs.
    // We'll do that later in the course.

    lengths.foreachRDD((rdd, _) => {

      val count = rdd.count()
      if (count > 0) {
        totalTweets.getAndAdd(count)
        totalChars.getAndAdd(rdd.reduce(_ + _))

        logger.info("Total tweets: " + totalTweets.get() +
          " Total characters: " + totalChars.get() +
          " Average: " + totalChars.get() / totalTweets.get())
      }
    })

    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint(checkpointDefaultDir)
    ssc
  }
}
