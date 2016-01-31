package edu.rosehulman.sanderib.clustering.util

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by isaac on 1/20/16.
 */
object DBScanNearestNeighborsHistogram {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    var rdd = ProcessGDELT.loadData(sc)

    val (buckets, bucketCounts) = rdd.cartesian(rdd)
      .map { pair: (Vector, Vector) => (pair._1, Vectors.sqdist(pair._1, pair._2)) }
      .groupByKey()
      .map { pair: (Vector, Iterable[Double]) => pair._2.min }.histogram(20)

    sc.parallelize(buckets.zip(bucketCounts)).saveAsTextFile("hdfs:///data/isaac/dbscan-parameter-tuning")
  }
}
