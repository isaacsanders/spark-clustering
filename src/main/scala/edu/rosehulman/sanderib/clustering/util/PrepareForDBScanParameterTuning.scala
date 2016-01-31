package edu.rosehulman.sanderib.clustering.util

import org.apache.spark.{SparkConf, SparkContext}

object PrepareForDBScanParameterTuning {
  val PARAMETER_TUNING_DATA_DIR = "hdfs:///data/isaac/dbscan-parameter-tuning/"
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val rdd = ProcessGDELT.loadData(sc).sample(false, 0.001)

    rdd.map(_.toArray.mkString(",")).saveAsTextFile(PARAMETER_TUNING_DATA_DIR)
  }
}
