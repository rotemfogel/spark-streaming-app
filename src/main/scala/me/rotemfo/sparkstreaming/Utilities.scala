package me.rotemfo.sparkstreaming

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Locale
import java.util.regex.{Matcher, Pattern}

import com.typesafe.config.ConfigFactory
import me.rotemfo.common.Logging

/**
 * project: spark-streaming-app
 * package: me.rotemfo.sparkstreaming
 * file:    Utilities
 * created: 2019-10-26
 * author:  Rotem
 */

object Utilities extends Logging {
  private final lazy val twitterConf = ConfigFactory.load().getConfig("twitter")
  final val datePattern = Pattern.compile("\\[(.*?) .+]")
  final val dateFormatter: SimpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)

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

  def parseDateField(field: String): Option[Timestamp] = {
    val dateMatcher: Matcher = datePattern.matcher(field)
    if (dateMatcher.find()) {
      val dateString = dateMatcher.group(1)
      try {
        val date = dateFormatter.parse(dateString)
        Some(Timestamp.from(Instant.ofEpochSecond(date.getTime)))
      } catch {
        case e: Throwable =>
          logger.error(s"error parsing date: $dateString", e)
          None
      }
    }
    else None
  }

  def now: Timestamp = Timestamp.from(Instant.now)
}
