package edu.rosehulman.sanderib.clustering.verification.external

import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

/**
 * Created by isaac on 1/18/16.
 */
abstract class ExternalMeasure extends Serializable {
  val data: RDD[Vector]
  val first: ClusteringModel
  val second: ClusteringModel
  var metric: Double = 0.0
  def run(): Unit
}
