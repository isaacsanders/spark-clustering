package edu.rosehulman.sanderib.clustering.verification.external

import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

class AdjustedRandIndex(override val data: RDD[Vector],
                        override val first: ClusteringModel,
                        override val second: ClusteringModel) extends ExternalMeasure {
  override def run(): Unit = {
    val sc = data.sparkContext
    val n = data.count()
    val nChoose2 = choose2(n)

    val predictions = data
      .map(vec => (first.predict(vec), second.predict(vec)))
      .countByValue()

    val n11: Long = predictions.map({ pair: ((Int, Int), Long) =>
      val ((a, b), inSame) = pair
      choose2(inSame)
    }).sum

    val n00: Long = predictions.map({ pair: ((Int, Int), Long) =>
      val ((a, b), inSame) = pair
      val notInSame = predictions.filterKeys({ pair: (Int, Int) =>
        val (c, d) = pair
        !(c == a || d == b)
      }).values.sum
      notInSame * inSame
    }).sum / 2


    this.metric = (n11 + n00).toDouble / nChoose2
  }

  def choose2(n: Long): Long = {
    n * (n - 1) / 2
  }
}
