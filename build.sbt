name := "clin-pdf-search"

version := "0.1"

scalaVersion := "2.12.8"

//libraryDependencies += "org.overviewproject" %% "pdfocr" % "0.0.10"

libraryDependencies += "net.sourceforge.tess4j" % "tess4j" % "4.0.0"

// https://mvnrepository.com/artifact/io.searchbox/jest-common
libraryDependencies += "io.searchbox" % "jest-common" % "0.1.1" 

libraryDependencies += "org.apache.opennlp" % "opennlp-tools" % "1.9.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

// https://mvnrepository.com/artifact/org.elasticsearch.client/elasticsearch-rest-high-level-client
libraryDependencies += "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "6.5.0"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-s3
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.549"

//https://www.playframework.com/documentation/2.7.x/ScalaJson
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.0"

libraryDependencies += "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.1.0-M3"

libraryDependencies += "com.typesafe.play" %% "play-ws-standalone-json" % "2.1.0-M3"

// https://mvnrepository.com/artifact/org.postgresql/postgresql
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.6"
