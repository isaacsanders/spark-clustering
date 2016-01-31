package edu.rosehulman.sanderib.clustering.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProcessGDELT {
  val DATA_DIR_PATH = "hdfs:///data/isaac/gdelt-processed/"
  val PATH_REGEX = "part-\\d{5}"

  // Indices
  val FRACTION_DATE_INDEX = 4
  val IS_ROOT_EVENT_INDEX = 25
  val EVENT_CODE_INDEX = 26
  val EVENT_BASE_CODE_INDEX = 27
  val EVENT_ROOT_CODE_INDEX = 28
  val QUAD_CLASS_INDEX = 29
  val GOLDSTEIN_SCALE_INDEX = 30
  val NUM_MENTIONS_INDEX = 31
  val NUM_SOURCES_INDEX = 32
  val NUM_ARTICLES_INDEX = 33
  val AVG_TONE_INDEX = 34
  val ACTOR1GEO_TYPE_INDEX = 35
  val ACTOR1GEO_LAT_INDEX = 39
  val ACTOR1GEO_LONG_INDEX = 40
  val ACTOR2GEO_TYPE_INDEX = 42
  val ACTOR2GEO_LAT_INDEX = 46
  val ACTOR2GEO_LONG_INDEX = 47

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ProcessGDELT")
    val sc = new SparkContext(conf)

    val indices = sc.broadcast(Array(
      FRACTION_DATE_INDEX,
      GOLDSTEIN_SCALE_INDEX,
      NUM_MENTIONS_INDEX,
      NUM_SOURCES_INDEX,
      NUM_ARTICLES_INDEX,
      AVG_TONE_INDEX,
      ACTOR1GEO_LAT_INDEX,
      ACTOR1GEO_LONG_INDEX,
      ACTOR2GEO_LAT_INDEX,
      ACTOR2GEO_LONG_INDEX))
    val vectors: RDD[Vector] = sc.wholeTextFiles("/data/isaac/gdelt", 40)
      .map(_._2)
      .flatMap(_.lines)
      .map(_.split('\t'))
      .map(indices.value.map(_))
      .filter(!_.contains(""))
      .map(_.map(x => x.toDouble))
      .map(Vectors.dense)
    vectors.saveAsObjectFile(DATA_DIR_PATH)
  }

  def loadData(sc: SparkContext): RDD[Vector] = {
    val filesystem = FileSystem.get(sc.hadoopConfiguration)

    var rdd: RDD[Vector] = null
    val iter = filesystem.listFiles(new Path(ProcessGDELT.DATA_DIR_PATH), true)
    while (iter.hasNext)  {
      val file = iter.next()
      if (file.getPath.getName.matches(ProcessGDELT.PATH_REGEX)) {
        val part: RDD[Vector] = sc.objectFile(file.getPath.toString)
        if (rdd == null) {
          rdd = part
        } else {
          rdd = rdd ++ part
        }

      }
    }

    return rdd
  }
}