package edu.rosehulman.sanderib.clustering.verification.external

import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IntMap
import scala.collection.parallel.mutable

class ClusterAccuracy(override val data: RDD[Vector],
                      override val first: ClusteringModel,
                      override val second: ClusteringModel) extends ExternalMeasure {
  def run(): Unit = {
    val totalCount = data.count()
    val vectorPredictionsToCounts: collection.Map[(Int, Int), Long] = data.map { vec: Vector =>
      ((first.predict(vec), second.predict(vec)), 1L)
    }.foldByKey(0L)(_+_).collectAsMap()

    val numberAccurate: Double = vectorPredictionsToCounts.map { (predictionsAndCount: ((Int, Int), Long)) =>
      val ((firstP, secondP), count) = predictionsAndCount
      IntMap((firstP, IntMap((secondP, count))))
    }.reduce({ (one: IntMap[IntMap[Long]], other: IntMap[IntMap[Long]]) =>
      var result = Map[Int, Map[Int, Long]]()
      one.unionWith[IntMap[Long]](other, { (key: Int, a: IntMap[Long], b: IntMap[Long]) =>
        a.unionWith[Long](b, { (_, x, y) => x + y })
      })
    }).toList.map({ pair: (Int, IntMap[Long]) =>
      val (outerKey: Int, countMap: IntMap[Long]) = pair
      val (innerKey, value) = countMap.maxBy(_._2)
      value.toDouble
    }).sum

    this.metric = numberAccurate / totalCount.toDouble
  }
}
