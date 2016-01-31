package edu.rosehulman.sanderib.clustering.algorithms

import java.util.Properties

import edu.rosehulman.sanderib.clustering._
import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.GaussianMixture
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.util.Random

object ExpectationMaximization {
  // Parameters
  val PROPERTY_PREFIX               = "edu.rosehulman.sanderib.clustering.algorithms.ExpectationMaximization"
  val NUM_CLUSTERS: String          = s"$PROPERTY_PREFIX.numClusters"
  val CONVERGENCE_TOLERANCE: String = s"$PROPERTY_PREFIX.convergenceTol"
  val MAX_ITERATIONS: String        = s"$PROPERTY_PREFIX.maxIterations"
  val SEED: String                  = s"$PROPERTY_PREFIX.seed"
}

class ExpectationMaximization(val data: RDD[Vector],
                              val numClusters: Int,
                              val convergenceTol: Double,
                              val maxIterations: Int,
                              val seed: Long) extends ClusteringAlgorithm {

  // Model
  var externalModel: GaussianMixture = null
  var centroids: Map[Int, Vector] = null
  var model: ClusteringModel = null

  def this(data: RDD[Vector], config: Properties) {
    this(
      data,
      config.getProperty(ExpectationMaximization.NUM_CLUSTERS, "2").toInt,
      config.getProperty(ExpectationMaximization.CONVERGENCE_TOLERANCE, "0.01").toDouble,
      config.getProperty(ExpectationMaximization.MAX_ITERATIONS, "100").toInt,
      config.getProperty(ExpectationMaximization.SEED, Random.nextLong().toString).toLong
    )
  }

  def run(): ClusteringModel = {
    this.externalModel = new GaussianMixture()
    this.externalModel.setK(this.numClusters)
    this.externalModel.setConvergenceTol(this.convergenceTol)
    this.externalModel.setMaxIterations(this.maxIterations)
    this.externalModel.setSeed(this.seed)

    this.centroids = ((1 to this.numClusters) zip this.externalModel.run(this.data).gaussians.map(x => x.mu.toDense)).toMap

    this.model = new ClusteringModel(this.centroids)
    this.model
  }
}