package me.rotemfo.sparkstreaming

import org.apache.spark.streaming._

/** Listens to a stream of tweets and saves them to disk. */
object SaveTweets extends BaseTwitterApp {

  override protected def contextWork(): StreamingContext = {
    getSparkStreamingContext()
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    //    val ssc = getSparkStreamingContext(duration = Seconds(5))
    //
    //    // Create a DStream from Twitter using our streaming context
    //    val tweets = TwitterUtils.createStream(ssc, None)
    //
    //    // Now extract the text of each status update into RDD's using map()
    //    val statuses = tweets.map(status => status.getText)
    //
    //    // Here's one way to just dump every partition of every stream to individual files:
    //    //statuses.saveAsTextFiles("Tweets", "txt")
    //
    //    // But let's do it the hard way to get a bit more control.
    //
    //    // Keep count of how many Tweets we've received so we can stop automatically
    //    // (and not fill up your disk!)
    //    var totalTweets: Long = 0
    //
    //    statuses.foreachRDD((rdd, time) => {
    //      // Don't bother with empty batches
    //      if (rdd.count() > 0) {
    //        // Combine each partition's results into a single RDD:
    //        val repartitionedRDD = rdd.repartition(1).cache()
    //        // And print out a directory with the results.
    //        repartitionedRDD.saveAsTextFile("Tweets_" + time.milliseconds.toString)
    //        // Stop once we've collected 1000 tweets.
    //        totalTweets += repartitionedRDD.count()
    //        println("Tweet count: " + totalTweets)
    //        if (totalTweets > 1000) {
    //          System.exit(0)
    //        }
    //      }
    //    })
    //
    //    // You can also write results into a database of your choosing, but we'll do that later.
    //
    //    // Set a checkpoint directory, and kick it all off
    //    ssc.start()
    //    ssc.awaitTermination()
  }
}
