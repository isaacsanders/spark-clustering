package edu.rosehulman.sanderib.clustering.algorithms

import java.util.Properties
import edu.rosehulman.sanderib.clustering.util.ClusteringModel
import org.alitouka.spark.dbscan.spatial.Point
import org.alitouka.spark.dbscan.util.io.IOHelper
import org.alitouka.spark.dbscan.{ClusterId, DbscanModel, Dbscan, DbscanSettings}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
 * Created by isaac on 1/4/16.
 */
object DBScan {
  // Parameters
  val PROPERTY_PREFIX: String              = "edu.rosehulman.sanderib.clustering.algorithms.DBScan"
  val EPSILON: String                      = s"$PROPERTY_PREFIX.epsilon"
  val NUMBER_OF_POINTS: String             = s"$PROPERTY_PREFIX.numberOfPoints"
  val TREAT_BORDER_POINTS_AS_NOISE: String = s"$PROPERTY_PREFIX.treatBorderPointsAsNoise"
}

class DBScan(val data: RDD[Vector],
             val epsilon: Double,
             val numberOfPoints: Int,
             val treatBorderPointsAsNoise: Boolean) extends ClusteringAlgorithm {

  var externalModel: DbscanModel = null
  var externalModelSettings: DbscanSettings = null

  override var model: ClusteringModel = null

  def this(data: RDD[Vector], config: Properties) {
    this(
      data,
      config.getProperty(DBScan.EPSILON, "1e-4").toDouble,
      config.getProperty(DBScan.NUMBER_OF_POINTS, "3").toInt,
      config.getProperty(DBScan.TREAT_BORDER_POINTS_AS_NOISE, "false").toBoolean
    )
  }

  override def run(): ClusteringModel = {
    val externalModelSettings = new DbscanSettings()
      .withEpsilon(this.epsilon)
      .withNumberOfPoints(this.numberOfPoints)
      .withTreatBorderPointsAsNoise(this.treatBorderPointsAsNoise)

    this.externalModel = Dbscan.train(data.map {v: Vector => new Point(v.toArray) }, externalModelSettings)

    val externalModel = this.externalModel
    val currentTime = System.nanoTime()
    val modelPath = s"/tmp/dbscan-$currentTime"
    val sc = data.sparkContext
    val dimensions = this.data.first().size

    val fn = { (entry1: (Vector, Int), entry2: (Vector, Int)) =>
      val ((a, n), (b, m)) = (entry1, entry2)
      val (ar, br) = (a.toArray, b.toArray)
      val sum = Vectors.dense(ar.zip(br).map({ pair =>
        val (x, y) = pair
        x + y
      }))
      (sum, n + m)
    }

    // Find centroids
    IOHelper.saveClusteringResult(externalModel, modelPath)
    val centroids = sc.wholeTextFiles(modelPath)
      .filter(_._1.contains("part-")) // Only use part-XXXXX files
      .map(_._2) // Take file contents
      .flatMap(_.lines) // Split into lines
      .map(_.split(",")) // Split each line into elements
      .map { entry => (entry.last.toInt, (Vectors.dense(entry.dropRight(1).map(_.toDouble)), 1)) } // Take cluster id from end and drop it from end for vector
      .reduceByKey(fn) // sum up vectors and counts
      .map { entry => val (clusterId, (sum, size)) = entry; (clusterId, Vectors.dense(sum.toArray.map(_/size))) } // find centroids
      .collectAsMap()

    this.model = new ClusteringModel(centroids)

    this.model
  }

}
