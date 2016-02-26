package edu.rosehulman.sanderib.clustering.algorithms

import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.apache.spark.Logging

/**
 * Created by isaac on 11/24/15.
 */
trait ClusteringAlgorithm extends Serializable {
  def run(): ClusteringModel
  var model: ClusteringModel
}
