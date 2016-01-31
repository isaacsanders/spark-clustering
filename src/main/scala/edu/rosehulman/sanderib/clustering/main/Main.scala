package edu.rosehulman.sanderib.clustering.main

import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties

import edu.rosehulman.sanderib.clustering.algorithms.{DBScan, ExpectationMaximization, FuzzyCMeans, KMeans}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Main {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Main")
    val sc = new SparkContext(conf)

    val cl = this.getClass.getClassLoader
    val stream = cl.getResource("s1.txt").openStream()
    val reader = new BufferedReader(new InputStreamReader(stream))

    var rawData = ListBuffer.empty[Array[Double]]
    var line: String = reader.readLine()
    do {
      rawData += line.split("\\s+").filterNot(_.isEmpty).map(_.toDouble)
      line = reader.readLine()
    } while (line != null)
    val data = rawData.toList.map(new DenseVector(_))

    val kmeansConfig = new Properties()
    kmeansConfig.load(cl.getResource("kmeans.properties").openStream())
    val fuzzyCMeansConfig = new Properties()
    fuzzyCMeansConfig.load(cl.getResource("fcm.properties").openStream())
    val emConfig = new Properties()
    emConfig.load(cl.getResource("em.properties").openStream())
    val dbscanConfig = new Properties()
    dbscanConfig.load(cl.getResource("dbscan.properties").openStream())

    val rdd: RDD[Vector] = sc.parallelize(data)

    val kmeans = new KMeans(rdd, kmeansConfig)
    val fuzzyCMeans = new FuzzyCMeans(rdd, fuzzyCMeansConfig)
    val em = new ExpectationMaximization(rdd, emConfig)
    val dbscan = new DBScan(rdd, dbscanConfig)

    kmeans.run()
    fuzzyCMeans.run()
    em.run()
    dbscan.run()

    System.out.println("Configs:")
    System.out.println(kmeansConfig)
    System.out.println(fuzzyCMeansConfig)
    System.out.println(emConfig)
    System.out.println(dbscanConfig)

    System.out.println("Centers:")
    System.out.println(kmeans.model.clusterCenters.toSeq.mkString(", "))
    System.out.println(fuzzyCMeans.model.clusterCenters.toSeq.mkString(", "))
    System.out.println(em.model.clusterCenters.toSeq.mkString(", "))
//    System.out.println(dbscan.model.clusterCenters.toSeq.mkString(", "))

    kmeans.model.save(sc, "/data/isaac/kmeans")
    fuzzyCMeans.model.save(sc, "/data/isaac/fcm")
    em.model.save(sc, "/data/isaac/em")
    System.out.println("[Main] Finished")
  }
}
