name := "clin-pdf-search"

version := "0.1"

scalaVersion := "2.12.8"

//libraryDependencies += "org.overviewproject" %% "pdfocr" % "0.0.10"

libraryDependencies += "net.sourceforge.tess4j" % "tess4j" % "4.0.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

// https://mvnrepository.com/artifact/org.elasticsearch.client/elasticsearch-rest-high-level-client
libraryDependencies += "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "6.5.0"