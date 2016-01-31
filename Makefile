build: src build.sbt
	sbt assembly

deploy: target
	scp target/scala-2.10/spark-clustering-assembly-1.0.jar hadoop-01.csse.rose-hulman.edu:
