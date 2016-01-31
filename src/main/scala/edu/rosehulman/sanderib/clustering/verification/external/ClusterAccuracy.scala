package edu.rosehulman.sanderib.clustering.verification.external

import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.parallel.mutable

class ClusterAccuracy(override val data: RDD[Vector],
                      override val first: ClusteringModel,
                      override val second: ClusteringModel) extends ExternalMeasure {
  def run(): Unit = {
    val totalCount = data.count()
    val labelAndClusterCounts = data.map { vec: Vector =>
      (first.predict(vec), second.predict(vec))
    }.countByValue

    val highestCountsPerCluster = mutable.ParHashMap[Int, Long]()
    labelAndClusterCounts.foreach {kv: ((Int, Int), Long) =>
      val ((label, cluster), count) = kv
      if (highestCountsPerCluster(cluster) < count) {
        highestCountsPerCluster(cluster) = count
      }
    }
    this.metric = highestCountsPerCluster.values.sum.toDouble / totalCount.toDouble
  }
}
