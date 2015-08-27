import AssemblyKeys._

name := "spark-twitter-lang-classifier"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.4.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.4.1"

libraryDependencies += "com.google.code.gson" % "gson" % "2.3"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.6"

libraryDependencies += "commons-cli" % "commons-cli" % "1.2"

libraryDependencies += "org.mongodb" %% "casbah" % "2.8.1"

libraryDependencies += "org.anormcypher" %% "anormcypher" % "0.6.0"

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "1.1.5"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "anormcypher" at "http://repo.anormcypher.org/"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.10" % "2.1.2"
libraryDependencies += "com.typesafe.akka" % "akka-kernel_2.10" % "2.1.2"
libraryDependencies += "com.typesafe.akka" % "akka-remote_2.10" % "2.1.2"
libraryDependencies += "org.jsoup" % "jsoup" % "1.8.2"

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
