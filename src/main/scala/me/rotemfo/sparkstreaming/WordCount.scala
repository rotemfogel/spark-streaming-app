package me.rotemfo.sparkstreaming

import org.apache.spark.sql.SparkSession

/** Create a RDD of lines from a text file, and keep count of
 * how often each word appears.
 */
object WordCount {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("WordCount").master("local[*]").getOrCreate()

    val workDir = getClass.getResource("/").getPath
    // Create a RDD of lines of text in our book
    val input = spark.read.text(s"$workDir/book.txt")
    // Use flatMap to convert this into an rdd of each word in each line
    val words = input.rdd.flatMap(line => line.getAs[String](0).split(' '))
    // Convert these words to lowercase
    val lowerCaseWords = words.map(word => word.toLowerCase())

    // Count up the occurrence of each unique word
    val wordCounts = lowerCaseWords.countByValue()

    // Sort by value descending and print the first 20 results
    val sample = wordCounts.toSeq.sortWith(_._2 > _._2).take(20)

    for ((word, count) <- sample) {
      println(word + " " + count)
    }

    spark.stop()
  }
}
