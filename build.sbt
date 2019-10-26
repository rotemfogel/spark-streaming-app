name := "spark-streaming-app"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.4.4"
  Seq(
    "com.typesafe"      % "config"           % "1.4.0",
    "org.apache.spark" %% "spark-sql"        % sparkVersion, // % "provided",
    "org.apache.spark" %% "spark-streaming"  % sparkVersion, // % "provided",
    "org.twitter4j"     % "twitter4j-stream" % "4.0.7"
  )
}