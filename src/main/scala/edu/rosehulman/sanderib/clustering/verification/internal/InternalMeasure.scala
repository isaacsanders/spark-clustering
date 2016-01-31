package edu.rosehulman.sanderib.clustering.verification.internal

import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Created by isaac on 1/18/16.
 */
abstract class InternalMeasure extends Serializable with Logging {
  val model: ClusteringModel
  val data: RDD[Vector]
  var metric: Double = 0.0
  def run(): Unit
}