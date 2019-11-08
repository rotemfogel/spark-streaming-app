package me.rotemfo.sparkstreaming.apache

import java.util.regex.Matcher

import me.rotemfo.sparkstreaming.Utilities
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * project: spark-streaming-app
 * package: me.rotemfo.sparkstreaming
 * file:    LogParser
 * created: 2019-11-08
 * author:  Rotem
 */
object LogParser {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = Utilities.apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // Extract the request field from each log line
    val requests: DStream[Option[String]] = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) Some(matcher.group(5))
      else None
    })

    // Extract the URL from the request
    val urls: DStream[Option[String]] = requests.filter(_.isDefined).map(x => {
      val arr = x.get.split(" ")
      if (arr.size == 3) Some(arr(1)) else None
    })

    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = urls.filter(_.isDefined).map(x => (x.get, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))
    sortedResults.print()

    // Kick it off
    ssc.checkpoint("checkpoint/LogParser")
    ssc.start()
    ssc.awaitTermination()
  }
}
