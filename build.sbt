name := "spark-streaming-app"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.4.4"
  Seq(
    "org.slf4j"         % "slf4j-api"        % "1.7.28",
    "ch.qos.logback"    % "logback-classic"  % "1.2.3",
    "com.typesafe"      % "config"           % "1.4.0",
    "org.apache.spark" %% "spark-sql"        % sparkVersion, // % "provided",
    "org.apache.spark" %% "spark-streaming"  % sparkVersion, // % "provided",
    "org.twitter4j" % "twitter4j-stream" % "4.0.7",
    "org.joda" % "joda-convert" % "2.2.1"
  )
}