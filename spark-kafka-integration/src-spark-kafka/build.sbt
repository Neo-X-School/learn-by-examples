name := "SparkKafkaStreaming"
version := "1.0"
val sparkVersion = "4.1.2"
scalaVersion := "2.13.18"

scalacOptions += "-deprecation"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "4.1.2"
)

libraryDependencies += "org.apache.kafka" %% "kafka" % "4.3.1"
