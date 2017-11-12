name := "HelloWorld"
 
version := "1.0"
 
scalaVersion := "2.11.8"
 
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided"

//fork := true