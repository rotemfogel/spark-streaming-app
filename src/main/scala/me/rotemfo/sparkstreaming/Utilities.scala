package me.rotemfo.sparkstreaming

import java.util.regex.Pattern

import com.typesafe.config.ConfigFactory

/**
 * project: spark-streaming-app
 * package: me.rotemfo.sparkstreaming
 * file:    Utilities
 * created: 2019-10-26
 * author:  Rotem
 */

object Utilities {

  private final lazy val twitterConf = ConfigFactory.load().getConfig("twitter")

  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter(): Unit = {
    val iter = twitterConf.entrySet().iterator()
    while (iter.hasNext) {
      val key = iter.next().getKey
      val value = twitterConf.getString(key)
      System.setProperty("twitter4j.oauth." + key, value)
    }
  }

  /** Retrieves a regex Pattern for parsing Apache access logs. */
  def apacheLogPattern(): Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }
}
