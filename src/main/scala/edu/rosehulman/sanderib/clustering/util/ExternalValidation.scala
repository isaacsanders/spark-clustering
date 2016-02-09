package edu.rosehulman.sanderib.clustering.util

import java.lang.reflect.Constructor

import edu.rosehulman.sanderib.clustering.verification.external.ExternalMeasure
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object ExternalValidation {
  val conf = new SparkConf().setAppName(this.getClass.getName)
  val sc = new SparkContext(conf)
  val filesystem = FileSystem.get(sc.hadoopConfiguration)
  val pattern = "/data/isaac/([^/]+)/([0-9.]+)/(\\d+)".r
  val data = ProcessGDELT.loadData(sc)

  def main(args: Array[String]) {
    val firstInputPath = args(0)
    val secondInputPath = args(1)

    val (firstAlgorithm, firstSamplingFraction, firstTimestamp) = inputPathParts(firstInputPath)
    val (secondAlgorithm, secondSamplingFraction, secondTimestamp) = inputPathParts(secondInputPath)

    val firstModel = loadModel(firstInputPath)
    val secondModel = loadModel(secondInputPath)

    val measureClassName = args(2)

    val constructor: Constructor[ExternalMeasure] = Class.forName(measureClassName).getDeclaredConstructors().find(ctor => ctor.getParameterTypes.last == firstModel.getClass) match {
      case Some(constructor: Constructor[ExternalMeasure]) => constructor
      case None => null
    }

    if (constructor != null) {
      val results = 1.to(100).par.map { _ =>
        val measure: ExternalMeasure = constructor.newInstance(data.sample(false, 0.001), firstModel, secondModel)
        measure.run()
        measure.metric
      }

      sc.parallelize(results.toList).saveAsTextFile(s"/data/isaac/$measureClassName/$firstAlgorithm/$firstSamplingFraction/$firstTimestamp/$secondAlgorithm/$secondSamplingFraction/$secondTimestamp")
    }
  }

  def loadModel(inputPath: String): ClusteringModel = {
    var modelRDD: RDD[(Int, Vector)] = null
    var iter = filesystem.listFiles(new Path(inputPath), true)

    while (iter.hasNext) {
      val file = iter.next()
      if (file.getPath.getName.matches(ProcessGDELT.PATH_REGEX)) {
        val part: RDD[(Int, Vector)] = sc.objectFile(file.getPath.toString)
        if (modelRDD == null) {
          modelRDD = part
        } else {
          modelRDD = modelRDD ++ part
        }
      }
    }

    val firstModelCentroids = modelRDD.collectAsMap()
    System.out.println(s"First Model Centroids: $firstModelCentroids")
    val model = new ClusteringModel(firstModelCentroids)
    return model
  }

  def inputPathParts(inputPath: String): (String, String, String) = {
    val (algorithm, samplingFraction, timestamp) = inputPath match {
      case pattern(algorithm, samplingFraction, timestamp) => (algorithm, samplingFraction, timestamp)
      case _ => (null, null, null)
    }

    return (algorithm, samplingFraction, timestamp)
  }
}
