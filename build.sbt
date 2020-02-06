name := "project5_spark"

version := "0.1"

scalaVersion := "2.11.8"

val confluentVersion = "3.1.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.3"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.3"

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.3" excludeAll(excludeJpountz)

libraryDependencies += "io.confluent"    % "kafka-schema-registry-client"  % confluentVersion
libraryDependencies += "io.confluent"    % "kafka-avro-serializer"         % confluentVersion
// https://mvnrepository.com/artifact/com.databricks/spark-avro
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"

resolvers ++= Seq(
  "Confluent" at "https://packages.confluent.io/maven",
  Resolver.mavenLocal
)
