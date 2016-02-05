package edu.rosehulman.sanderib.clustering.verification.internal

import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD

class DaviesBouldinIndex(override val data: RDD[Vector],
                         override val model: ClusteringModel) extends InternalMeasure {
  override def run(): Unit = {
    val sc = data.sparkContext
    val groupedData: RDD[(Int, Iterable[Vector])] = data.groupBy(model.predict)
    val clusterIds: RDD[Int] = sc.parallelize(model.centroids.keySet.toList)
    val clusterIdPairs: RDD[(Int, Int)] = clusterIds.cartesian(clusterIds).filter(pair => pair._1 != pair._2)

    val compactness: collection.Map[Int, Double] = groupedData.map { cluster: (Int, Iterable[Vector]) =>
      val (clusterId, points) = cluster
      val centroid = model.centroids.get(clusterId) match {
        case Some(centroid: Vector) => centroid
        case None => null
      }

      val compactness = points.map({ p => Math.sqrt(Vectors.sqdist(p, centroid)) }).sum / points.size

      (clusterId, compactness)
    }.collectAsMap()

    val maxes: RDD[Double] = clusterIdPairs.map { clusterIdPair: (Int, Int) =>
      val (i, j) = clusterIdPair
      val ci = model.centroids.get(i).get
      val cj = model.centroids.get(j).get
      val indexValue = (compactness.getOrElse[Double](i, 0.0) + compactness.getOrElse[Double](j, 0.0)) / Math.sqrt(Vectors.sqdist(ci, cj))

      (i, indexValue)
    }.groupByKey().mapValues(_.max).values
    this.metric = maxes.sum() / model.k
  }
}
