package edu.rosehulman.sanderib.clustering.algorithms

import java.util.Properties
import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

import scala.util.Random

object KMeans {
  // Parameters
  val PROPERTY_PREFIX: String       = "edu.rosehulman.sanderib.clustering.algorithms.KMeans"
  val NUM_CLUSTERS: String          = s"$PROPERTY_PREFIX.numClusters"
  val CONVERGENCE_TOLERANCE: String = s"$PROPERTY_PREFIX.convergenceTol"
  val INITIALIZATION_MODE: String   = s"$PROPERTY_PREFIX.initializationMode"
  val INITIALIZATION_STEPS: String  = s"$PROPERTY_PREFIX.initializationSteps"
  val MAX_ITERATIONS: String        = s"$PROPERTY_PREFIX.maxIterations"
  val RUNS: String                  = s"$PROPERTY_PREFIX.runs"
  val SEED: String                  = s"$PROPERTY_PREFIX.seed"
}

class KMeans(val data: RDD[Vector],
             val numClusters: Int,
             val convergenceTol: Double,
             val initializationMode: String,
             val initializationSteps: Int,
             val maxIterations: Int,
             val runs: Int,
             val seed: Long) extends ClusteringAlgorithm {

  // Model
  var externalModel: org.apache.spark.mllib.clustering.KMeans = null
  var centroids: Map[Int, DenseVector] = null
  var model: ClusteringModel = null

  def this(data: RDD[Vector], config: Properties) {
    this(
      data,
      config.getProperty(KMeans.NUM_CLUSTERS, "2").toInt,
      config.getProperty(KMeans.CONVERGENCE_TOLERANCE, "0.01").toDouble,
      config.getProperty(KMeans.INITIALIZATION_MODE, "random"),
      config.getProperty(KMeans.INITIALIZATION_STEPS, "5").toInt,
      config.getProperty(KMeans.MAX_ITERATIONS, "100").toInt,
      config.getProperty(KMeans.RUNS, "5").toInt,
      config.getProperty(KMeans.SEED, Random.nextLong().toString).toLong
    )
  }

  override def run(): ClusteringModel = {
    this.externalModel = new org.apache.spark.mllib.clustering.KMeans()
    this.externalModel.setK(this.numClusters)
    this.externalModel.setEpsilon(this.convergenceTol)
    this.externalModel.setInitializationMode(this.initializationMode)
    this.externalModel.setInitializationSteps(this.initializationSteps)
    this.externalModel.setMaxIterations(this.maxIterations)
    this.externalModel.setRuns(this.runs)
    this.externalModel.setSeed(this.seed)

    this.centroids = ((1 to this.numClusters) zip this.externalModel.run(this.data).clusterCenters.map(_.toDense)).toMap

    this.model = new ClusteringModel(this.centroids)
    this.model
  }

  override def toString(): String = {
    s"KMeans{numClusters=$numClusters,"+
      s"convergenceTol=$convergenceTol,"+
      s"initializationMode=$initializationMode,"+
      s"initializationSteps=$initializationSteps,"+
      s"maxIterations=$maxIterations,"+
      s"runs=$runs,"+
      s"seed=$seed}"
  }
}
