package edu.rosehulman.sanderib.clustering.verification.internal

import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

class Compactness(override val data: RDD[Vector],
                  override val model: ClusteringModel) extends InternalMeasure {
  def run(): Unit = {
    val groupedData = data.groupBy(model.predict)
    val compactness = groupedData.map({ cluster: (Int, Iterable[Vector]) =>
      val (centroidId, points) = cluster
      val centroid = model.centroids.get(centroidId) match {
        case Some(centroid: Vector) => centroid
        case None => null
      }

      val compactness = points.map({ p => Math.sqrt(Vectors.sqdist(p, centroid)) }).sum / points.size

      compactness
    }).sum() / groupedData.count()

    this.metric = compactness
  }
}
