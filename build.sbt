name := "spark-streaming-app"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.4.4"
  val kafkaVersion = "2.1.1"
  Seq(
    "org.slf4j"           % "slf4j-api"                   % "1.7.28",
    "ch.qos.logback"      % "logback-classic"             % "1.2.3",
    "com.typesafe"        % "config"                      % "1.4.0",
    "org.apache.spark"   %% "spark-sql"                   % sparkVersion exclude("org.slf4j", "slf4j-log4j12"), // % "provided",
    "org.apache.spark"   %% "spark-streaming"             % sparkVersion exclude("org.slf4j", "slf4j-log4j12"), // % "provided",
    "org.apache.kafka"   %% "kafka"                       % kafkaVersion,
    "org.apache.spark"   %% "spark-streaming-kafka-0-10"  % sparkVersion exclude("org.slf4j", "slf4j-log4j12"),
    "org.apache.spark"   %% "spark-streaming-flume"       % sparkVersion exclude("org.slf4j", "slf4j-log4j12"),
    "org.apache.spark"   %% "spark-streaming-kinesis-asl" % sparkVersion excludeAll (ExclusionRule("org.slf4j", "slf4j-log4j12"), ExclusionRule("com.amazonaws", "amazon-kinesis-client")),
    "com.datastax.spark" %% "spark-cassandra-connector"   % "2.4.2",
    "com.amazonaws"       % "amazon-kinesis-client"       % "1.13.0",
    "org.twitter4j"       % "twitter4j-stream"            % "4.0.7",
    "org.joda"            % "joda-convert"                % "2.2.1",

    "org.specs2"         %% "specs2-core"                 % "4.6.0" % Test
  )
}

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"
