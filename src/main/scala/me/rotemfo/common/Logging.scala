package me.rotemfo.common

import org.slf4j.{Logger, LoggerFactory}

/**
 * project: spark-streaming-app
 * package: me.rotemfo.common
 * file:    Logging
 * created: 2019-11-27
 * author:  Rotem
 */
trait Logging {
  protected final val logger: Logger = LoggerFactory.getLogger(getClass)

}
