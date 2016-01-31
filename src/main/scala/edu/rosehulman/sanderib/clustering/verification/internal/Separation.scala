package edu.rosehulman.sanderib.clustering.verification.internal

import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

class Separation(override val data: RDD[Vector],
                 override val model: ClusteringModel) extends InternalMeasure {
  def run(): Unit = {
    val sc = data.sparkContext
    val centroids = sc.parallelize(model.clusterCenters)
    val centroidPairs: RDD[(Vector, Vector)] = centroids.cartesian[Vector](centroids)
    this.metric = centroidPairs.filter(pair => !pair._1.equals(pair._2))
      .map(pair => Math.sqrt(Vectors.sqdist(pair._1, pair._2))).sum() * (2.0 / (Math.pow(model.k, 2) - model.k))
  }
}
