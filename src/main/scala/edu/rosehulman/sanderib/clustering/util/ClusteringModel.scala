package edu.rosehulman.sanderib.clustering.util

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD

import scala.collection.Map

class ClusteringModel(var centroids: Map[Int, Vector]) extends Serializable {
  def clusterCenters: Array[Vector] = {
    this.centroids.values.toArray
  }

  def k: Int = {
    return this.centroids.size
  }

  def predict(points: RDD[Vector]): RDD[Int] = {
    points map {
      this.predict(_)
    }
  }

  def predict(point: Vector): Int = {
    val (cluster, _) = this.centroids.mapValues(Vectors.sqdist(_, point)).minBy(_._2)
    return cluster
  }

  def save(sc: SparkContext, path: String): Unit = {
    sc.parallelize[(Int, Vector)](this.centroids.toList).saveAsObjectFile(path)
  }
}