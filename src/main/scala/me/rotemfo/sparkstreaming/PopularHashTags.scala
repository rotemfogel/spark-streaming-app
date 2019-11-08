package me.rotemfo.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
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

  override protected def contextWork(): StreamingContext = {
    val ssc = getSparkStreamingContext(duration = Seconds(5))

    // Create a DStream from Twitter using our streaming context
    val tweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)

    val words: DStream[String] = tweets.flatMap(s => {
      s.getHashtagEntities.map(_.getText) ++ s.getText.split(" ")
        .flatMap(_.split(" "))
    })

    // Now eliminate anything that's not a hashTag && remove pound sign
    val hashTags = words.filter(word => word.startsWith("#")).map(_.substring(1))

    // Map each hashTag to a key/value pair of (hashTag, 1) so we can count them up by adding up the values
    val hashTagsKeyValues = hashTags.map((_, 1))

    // Now count them up over a 5 minute window sliding every one second
    val hashTagsCounts = hashTagsKeyValues.reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(30))

    //  You will often see this written in the following shorthand:

    // Sort the results by the count values
    val sortedResults = hashTagsCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))

    // Print the top 10
    sortedResults.print

    ssc.checkpoint(checkpointDefaultDir)
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