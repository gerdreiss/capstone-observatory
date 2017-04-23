package observatory

import java.lang.Math._

import org.apache.spark.sql.types.DataTypes.{DoubleType, IntegerType}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders}

import scala.math.{Pi, atan, max, min, sinh}
import scala.reflect.ClassTag

object implicits {

  implicit class DoubleToRGB(value: Double) {
    def toRGB: Int = max(0, min(255, math.round(value).toInt))
  }

  implicit class F2C(f: Double) {
    def toCelsius: Double = (f - 32) * 5 / 9
  }

  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  implicit def tuple3[A1, A2, A3](implicit e1: Encoder[A1], e2: Encoder[A2], e3: Encoder[A3]): Encoder[(A1, A2, A3)] =
    Encoders.tuple[A1, A2, A3](e1, e2, e3)
}

case class Location(lat: Double, lon: Double) {
  private val R = 6371e3
  private val p = 2

  private def distanceTo(loc: Location): Double = {
    val dLat = (loc.lat - lat).toRadians
    val dLon = (loc.lon - lon).toRadians

    val a = sin(dLat / 2) * sin(dLat / 2) + cos(lat.toRadians) * cos(loc.lat.toRadians) * sin(dLon / 2) * sin(dLon / 2)
    val c = 2 * atan2(sqrt(a), sqrt(1 - a))

    R * c
  }

  def idw(loc: Location): Double = {
    1 / pow(distanceTo(loc), p)
  }

  def isAt(x: Int, y: Int): Boolean = {
    lat.toInt == x && lon.toInt == y
  }
}

object Location {
  def fromPixelIndex(index: Int): Location = {
    def x: Int = index % 360
    def y: Int = index / 360

    Location(90 - y, x - 180)
  }

  def fromPixelIndexZoomXY(index: Int, zoom: Int, x: Int, y: Int): Location = {
    def x0: Int = (index % 256) / 256 + x
    def y0: Int = (index / 256) / 256 + y

    val p: Int = 1 << zoom

    def lat: Double = atan(sinh(Pi * (1.0 - 2.0 * y0 / p))) * 180.0 / Pi
    def lon: Double = x0 * 360.0 / p - 180.0

    Location(lat, lon)
  }
}

case class Color(red: Int, green: Int, blue: Int)

//case class Station(stn: Int, wban: Int, lat: Double, lon: Double)
//object Station {
//  def valid(fields: Array[String]): Boolean = {
//    fields.length == 4 && fields(2).nonEmpty && fields(3).nonEmpty
//  }
//  def parse(fields: Array[String]): Station = {
//    Station(
//      stn = Try(fields(0).toInt).getOrElse(0),
//      wban = Try(fields(1).toInt).getOrElse(0),
//      lat = fields(2).toDouble,
//      lon = fields(3).toDouble)
//  }
//}

case class Station(stn: Option[Int], wban: Option[Int], lat: Option[Double], lon: Option[Double])

object Station {

  val structType = StructType(Seq(
    StructField("stn", IntegerType, nullable = true),
    StructField("wban", IntegerType, nullable = true),
    StructField("lat", DoubleType, nullable = true),
    StructField("lon", DoubleType, nullable = true)
  ))
}

//case class Record(stn: Int, wban: Int, month: Int, day: Int, temp: Double)
//object Record {
//  def valid(fields: Array[String]): Boolean = {
//    fields.length == 5 && fields(2).nonEmpty && fields(3).nonEmpty && fields(4).nonEmpty
//  }
//  def parse(fields: Array[String]): Record = {
//    Record(
//      stn = Try(fields(0).toInt).getOrElse(0),
//      wban = Try(fields(1).toInt).getOrElse(0),
//      month = fields(2).toInt,
//      day = fields(3).toInt,
//      temp = fields(4).toCelsius)
//  }
//}

case class Record(stn: Option[Int], wban: Option[Int], month: Int, day: Int, temp: Double)

object Record {
  val structType = StructType(Seq(
    StructField("stn", IntegerType, nullable = true),
    StructField("wban", IntegerType, nullable = true),
    StructField("month", IntegerType, nullable = false),
    StructField("day", IntegerType, nullable = false),
    StructField("temp", DoubleType, nullable = false)
  ))
}

