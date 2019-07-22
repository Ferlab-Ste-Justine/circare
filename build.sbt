name := "circare"

version := "0.1"

scalaVersion := "2.12.8"

//libraryDependencies += "org.overviewproject" %% "pdfocr" % "0.0.10"

libraryDependencies += "net.sourceforge.tess4j" % "tess4j" % "4.0.0"

// https://mvnrepository.com/artifact/org.elasticsearch.client/elasticsearch-rest-high-level-client
libraryDependencies += "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "6.5.0"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.3"

libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
