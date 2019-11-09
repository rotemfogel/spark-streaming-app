package me.rotemfo.sparkstreaming.apache

import java.io.{File, FileInputStream}
import java.util.regex.Matcher

import me.rotemfo.sparkstreaming.Utilities
import org.specs2.mutable.Specification

import scala.io.Source

/**
 * project: spark-streaming-app
 * package: me.rotemfo.sparkstreaming.apache
 * file:    DateParseSpec
 * created: 2019-11-09
 * author:  Rotem
 */
class DateParseSpec extends Specification {

  "should parse dates" >> {
    val source = Source.fromInputStream(new FileInputStream(new File("logs/access_log.txt")))
    try {
      val lines = source.getLines().toSeq

      lines.foreach(line => {
        val matcher: Matcher = logPattern.matcher(line)
        if (matcher.matches) {
          val result = Utilities.parseDateField(matcher.group(4))
          result mustNotEqual None
        }
      })
      1 mustEqual 1
    } finally {
      source.close
    }
  }
}
