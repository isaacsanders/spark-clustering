name := "spark-clustering"

version := "1.0"

moduleID := "edu.rosehulman.sanderib" % "spark-clustering" % "1.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.2"

libraryDependencies += "org.alitouka" % "spark_dbscan_2.10" % "0.0.5-SNAPSHOT"

scalacOptions += "-feature"

scalacOptions += "-deprecation"

assemblyMergeStrategy in assembly := {
  case PathList("meta-inf", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
