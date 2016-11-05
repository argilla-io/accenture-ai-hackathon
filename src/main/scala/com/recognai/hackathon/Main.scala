package com.recognai.hackathon

import java.time.format.DateTimeFormatter

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * Created by @frascuchon on 04/11/2016.
  */
object Main extends App {

  type Point = (Double, Double)

  def distance(a: Point, b: Point): Double = {
    import math._
    sqrt(pow(a._1 - b._1, 2) + pow(a._2 - b._2, 2))
  }


  val session = SparkSession.builder()
    .appName("Data processing")
    .getOrCreate()

  val conf = new SparkConf().setAppName("Data processing")
  val sc = session.sparkContext

  val pollutionPath = StringUtils.defaultIfBlank(args(0), "aiHackathon/data/test/contaminacion_subset.txt")
  val trafficPath = StringUtils.defaultIfBlank(args(1), "aiHackathon/data/test/trafico_subset.txt")
  val outputfilePath = StringUtils.defaultIfBlank(args(2), "pollution_data")
  val RATIO = conf.get("distance.ratio", "0.04").toDouble

  import session.implicits._

  val dateFormat = DateTimeFormatter.ofPattern("YYYY-MM-DD HH:mm:SS")

  private def toDouble(value: String): Double = Try(value.toDouble).toOption.getOrElse(0.0)

  // Prepare pollution data
  val pollution = sc.textFile(pollutionPath)
    .map(line => {
      val fields = line.split(",")

      // id_estación latitud longitud easting northing fecha NO2 PM25 PM10 O3
      (fields(0).toLong, new Point(fields(1).toDouble, fields(2).toDouble), fields(5),
        toDouble(fields(6)), toDouble(fields(7)), toDouble(fields(8)), toDouble(fields(9)))
    })
    .toDS()

  // Prepare traffic data
  val traffic = sc.textFile(trafficPath)
    .map(line => {
      val fields = line.split(",")

      //id_elemento tipo_elemento easting northing latitud longitud fecha intensidad ocupacion carga velocidad_media error muestras_periodo
      (fields(0).toLong, new Point(fields(4).toDouble, fields(5).toDouble), fields(6), toDouble(fields(7)),
        toDouble(fields(8)), toDouble(fields(9)), toDouble(fields(10)), fields(11), fields(12).toLong)
    })
    .toDS()
    .cache()

  // calculate distances
  val distancesTable = pollution.map((_, 0)).as("pollution_0")
    .joinWith(traffic.map((_, 0)).as("traffic_0"), $"pollution_0._2" === $"traffic_0._2")
    .map {
      case ((t1, _), (t2, _)) => (t1._1, t2._1, distance(t1._2, t2._2))
    }

  val filteredDistances = distancesTable.filter(_._3 <= RATIO)

  val pollutionWithDistances = pollution.as("pollution")
    .joinWith(filteredDistances.as("distances"), $"pollution._1" === $"distances._1")


  val pollutionWithTrafficInformation = pollutionWithDistances.as("pollution_d")
    .joinWith(traffic.as("traffic"), $"pollution_d._2._2" === $"traffic._1" && $"pollution_d._1._3" === $"traffic._3")

  pollutionWithTrafficInformation
    .map {
      case ((pollution, (_, _, distance)), traffic) =>
        (pollution._1, pollution._3, pollution._4, pollution._5, pollution._6, pollution._7,
          distance,
          traffic._1, traffic._3, traffic._4, traffic._5, traffic._6, traffic._7, traffic._8, traffic._9)
    }
    .rdd
    .map(_.productIterator.mkString(","))
    .saveAsTextFile(outputfilePath)
}
