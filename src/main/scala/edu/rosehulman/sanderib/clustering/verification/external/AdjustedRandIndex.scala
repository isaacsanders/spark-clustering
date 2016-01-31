package edu.rosehulman.sanderib.clustering.verification.external

import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

class AdjustedRandIndex(override val data: RDD[Vector],
                        override val first: ClusteringModel,
                        override val second: ClusteringModel) extends ExternalMeasure {
  override def run(): Unit = {
    val sc = data.sparkContext
    val dataPairs = data.cartesian(data)
    val n = data.count()

    val nXX = dataPairs.map { pair: (Vector, Vector) =>
      val (one, other) = pair
      val sameInFirst: Boolean = first.predict(one) == first.predict(other)
      val sameInSecond: Boolean = second.predict(one) == second.predict(other)
      (sameInFirst, sameInSecond) // n11 and n00
    }.countByValue()

    val n11 = nXX.get((true, true)).get
    val n00 = nXX.get((false, false)).get

    this.metric = (2 * (n11 + n00).toDouble) / (n * (n - 1))
  }
}
