package me.rotemfo.sparkstreaming

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status

/**
 * project: spark-streaming-app
 * package: me.rotemfo.sparkstreaming
 * file:    PopularHashTags
 * created: 2019-11-01
 * author:  Rotem
 */
/** Simple application to listen to a stream of Tweets and print them out */
object PopularHashTags extends BaseTwitterApp {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = getSparkStreamingContext()

    // Create a DStream from Twitter using our streaming context
    val tweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)

    val words: DStream[String] = tweets.flatMap(s => {
      s.getHashtagEntities.map(_.getText) ++ s.getText.split(" ")
    })

    // Now eliminate anything that's not a hashtag
    val hashTags = words.filter(word => word.startsWith("#"))

    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val hashTagsKeyValues = hashTags.map((_, 1))

    // Now count them up over a 5 minute window sliding every one second
    val hashTagsCounts = hashTagsKeyValues.reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(300), Seconds(5))

    //  You will often see this written in the following shorthand:
    //val hashTagsCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))

    // Sort the results by the count values
    val sortedResults = hashTagsCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))

    // Print the top 10
    sortedResults.print

    // Kick it all off
    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}