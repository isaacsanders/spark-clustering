package edu.rosehulman.sanderib.clustering.util

import java.lang.reflect.Constructor

import edu.rosehulman.sanderib.clustering.algorithms.ClusteringAlgorithm
import edu.rosehulman.sanderib.clustering.verification.internal.{InternalMeasure, Separation, Compactness}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.util.matching.Regex

/**
 * Created by isaac on 1/24/16.
 */
object InternalValidation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val filesystem = FileSystem.get(sc.hadoopConfiguration)

    val inputPath = args(0)

    val pattern = "/data/isaac/([^/]+)/([0-9.]+)/(\\d+)".r
    val (algorithm, samplingFraction, timestamp) = inputPath match {
      case pattern(algorithm, samplingFraction, timestamp) => (algorithm, samplingFraction, timestamp)
      case _ => (null, null, null)
    }

    var rdd: RDD[(Int, Vector)] = null
    val iter = filesystem.listFiles(new Path(inputPath), true)
    while (iter.hasNext)  {
      val file = iter.next()
      if (file.getPath.getName.matches(ProcessGDELT.PATH_REGEX)) {
        val part: RDD[(Int, Vector)] = sc.objectFile(file.getPath.toString)
        if (rdd == null) {
          rdd = part
        } else {
          rdd = rdd ++ part
        }
      }
    }

    val centroids = rdd.collectAsMap()
    System.out.println(s"Centroid: $centroids")
    val model = new ClusteringModel(centroids)
    val data = ProcessGDELT.loadData(sc)

    val measureClassName = args(1)

    val constructor: Constructor[InternalMeasure] = Class.forName(measureClassName).getDeclaredConstructors().find(ctor => ctor.getParameterTypes.last == model.getClass) match {
      case Some(constructor: Constructor[InternalMeasure]) => constructor
      case None => null
    }

    if (constructor != null) {
      val results = 1.to(100).par.map { _ =>
        val measure: InternalMeasure = constructor.newInstance(data.sample(false, 0.001), model)
        measure.run()
        measure.metric
      }

      sc.parallelize(results.toList).saveAsTextFile(s"/data/isaac/$measureClassName/$algorithm/$samplingFraction/$timestamp")
    }
  }
}
