package me.rotemfo.sparkstreaming.twitter

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.DateTime
import twitter4j.Status

/** Simple application to listen to a stream of Tweets and print them out */
object CountByUserCreationYear extends BaseTwitterApp {

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    val context: StreamingContext = contextWork()

    // Kick it all off
    context.start()
    context.awaitTermination()
  }

  override protected def contextWork(): StreamingContext = {
    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = getSparkStreamingContext()

    // Create a DStream from Twitter using our streaming context
    val tweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)

    val totalByUser = tweets.map(t => {
      val createdAt = t.getUser.getCreatedAt
      new DateTime(createdAt.getTime).year().get()
    }).countByValueAndWindow(Seconds(10), Seconds(10))

    totalByUser.foreachRDD(rdd => rdd.take(30).foreach(r => logger.info("({}, {})", r._1, r._2)))
    ssc.checkpoint(checkpointDefaultDir)
    ssc
  }
}
