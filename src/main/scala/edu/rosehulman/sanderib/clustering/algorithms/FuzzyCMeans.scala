package edu.rosehulman.sanderib.clustering.algorithms

import java.util.Properties

import edu.rosehulman.sanderib.clustering._
import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors, DenseVector, Vector}
import org.apache.spark.rdd.RDD

import scala.util.Random

object FuzzyCMeans {
  // Parameters
  val PROPERTY_PREFIX: String       = "edu.rosehulman.sanderib.clustering.algorithms.FuzzyCMeans"
  val NUM_CLUSTERS: String          = s"$PROPERTY_PREFIX.numClusters"
  val WEIGHTING_EXPONENT: String    = s"$PROPERTY_PREFIX.weightingExponent"
  val SENSITIVITY_THRESHOLD: String = s"$PROPERTY_PREFIX.sensitivityThreshold"
}

class FuzzyCMeans(val data: RDD[Vector],
                  val numClusters: Int,
                  val weightingExponent: Double,
                  val sensitivityThreshold: Double) extends ClusteringAlgorithm {

  // Model
  var centroids: collection.Map[Int, Vector] = null
  var model: ClusteringModel = null
  var globalMax: Double = Double.MinPositiveValue

  def this(data: RDD[Vector], config: Properties) {
    this(
      data,
      config.getProperty(FuzzyCMeans.NUM_CLUSTERS, "2").toInt,
      config.getProperty(FuzzyCMeans.WEIGHTING_EXPONENT, "2.0").toDouble,
      config.getProperty(FuzzyCMeans.SENSITIVITY_THRESHOLD, "0.1").toDouble
    )
  }

  def run(): ClusteringModel = {
    val sc = data.sparkContext
    var clusteredData = data flatMap {(vec: Vector) =>
      val clusters = 1.to(numClusters)

      val weights = clusters map {(_) => Math.abs(Random.nextGaussian()) }
      val totalWeight: Double = weights.sum
      val normalizedWeights = weights.map { _ / totalWeight }

      clusters.map {(i: Int) =>
        ((i, vec), normalizedWeights(i-1))
      }
    }

    do {
      val weights = clusteredData map {kv: ((Int, Vector), Double) =>
        val ((j, _), u) = kv
        (j, Math.pow(u, this.weightingExponent))
      }

      val weightedVectors: RDD[(Int, Vector)] = clusteredData map {kv: ((Int, Vector), Double) =>
        val ((j, x), u): ((Int, Vector), Double) = kv
        (j, Vectors.dense(x.toArray map {(d: Double) => Math.pow(u, this.weightingExponent) * d }))
      }

      val totaledWeights = weights.foldByKey(0)(_+_).collectAsMap()
      val totaledWeightedVectors = weightedVectors.reduceByKey {(one: Vector, other: Vector) =>
        Vectors.dense((one.toArray zip other.toArray) map ((p: (Double, Double)) => p._1 + p._2))
      }.collectAsMap()

      this.centroids = totaledWeightedVectors.map { kv: (Int, Vector) =>
        val (j, v) = kv
        (j, new DenseVector(v.toArray map { (d: Double) => d / totaledWeights(j)}))
      }.toMap

      val clusteredDataWithDistances: RDD[((Int, Vector), (Double, collection.Map[Int, Double]))] = clusteredData.map { kv: ((Int, Vector), Double) =>
        val ((j, x), u) = kv
        val distances = this.centroids.mapValues[Double] { centroid =>
          Math.sqrt(Vectors.sqdist(x, centroid))
        }
        ((j, x), (u, distances))
      }



      val crossGenClusteredData = clusteredDataWithDistances.map {kv: ((Int, Vector), (Double, collection.Map[Int, Double])) =>
        val ((j, x), (oldU, distances)) = kv

        val dji: Double = distances(j)
        val newU = Math.pow(distances.values.map {(dki: Double) =>
          Math.pow(dji / dki, 2.0 / (this.weightingExponent - 1))
        }.sum, -1)

        ((j, x), (oldU, newU))
      }

      clusteredData = crossGenClusteredData.mapValues(_._2).cache()

      val localMax = sc.broadcast(crossGenClusteredData.values.map { pair: (Double, Double) => pair._2 - pair._1 }.max())
      this.globalMax = localMax.value
      logInfo(s"Error updated: $globalMax")
    } while (this.globalMax >= this.sensitivityThreshold)

    this.model = new ClusteringModel(this.centroids)
    this.model
  }
}
