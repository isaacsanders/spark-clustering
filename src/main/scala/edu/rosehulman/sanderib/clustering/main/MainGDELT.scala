package edu.rosehulman.sanderib.clustering.main

import java.lang.reflect.Constructor
import java.util.Properties

import edu.rosehulman.sanderib.clustering.algorithms._
import edu.rosehulman.sanderib.clustering.util.ProcessGDELT
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object MainGDELT {


  def main(args: Array[String]) {
    if (args.length < 2) {
      throw new Exception("USAGE: cluster_gdelt ALGORITHM_CLASS_NAME ALGORITHM_CONFIG_PATH [SAMPLING_FRACTION]")
    }

    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    var rdd = ProcessGDELT.loadData(sc)
    val filesystem = FileSystem.get(sc.hadoopConfiguration)

    System.out.println(rdd.count())
    var samplingFraction = 1.0
    if (args.length > 2) {
      samplingFraction = args(2).toDouble
      rdd = rdd.sample(false, samplingFraction, System.nanoTime() % 100)
    }
    System.out.println(rdd.count())

    val modelConfigPath = new Path(args(1))
    val modelConfigInputStream = filesystem.open(modelConfigPath)

    val modelConfig = new Properties()
    modelConfig.load(modelConfigInputStream)

    val modelClassName = args(0)

    val constructor: Constructor[ClusteringAlgorithm] = Class.forName(modelClassName).getDeclaredConstructors().find(ctor => ctor.getParameterTypes.last == modelConfig.getClass) match {
      case Some(constructor: Constructor[ClusteringAlgorithm]) => constructor
      case None => null
    }

    val startTime = sc.startTime
    if (constructor != null) {
      val model: ClusteringAlgorithm = constructor.newInstance(rdd, modelConfig)
      model.run()
      model.model.save(sc, s"/data/isaac/$modelClassName/$samplingFraction/$startTime")
    }
  }
}
