package observatory

import java.lang.Math._

import org.apache.spark.sql.{Encoder, Encoders}

import scala.math.{max, min}
import scala.reflect.ClassTag
import scala.util.Try

object implicits {

  implicit class DoubleToRGB(value: Double) {
    def toRGB: Int = max(0, min(255, math.round(value).toInt))
  }

  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  implicit def tuple3[A1, A2, A3](implicit e1: Encoder[A1], e2: Encoder[A2], e3: Encoder[A3]): Encoder[(A1, A2, A3)] =
    Encoders.tuple[A1, A2, A3](e1, e2, e3)
}

case class Coord(x: Int, y: Int)

case class Location(lat: Double, lon: Double) {
  private val R = 6371e3 // metres
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
}

object Location {
  def fromCoord(coord: Coord): Location = {
    Location(90 - (coord.y / 2), (coord.x / 2) - 180)
  }

  def fromPixelIndex(index: Int): Location = {
    def x: Int = index % 360
    def y: Int = index / 360

    Location(90.0 - y, x - 180)
  }
}

case class Color(red: Int, green: Int, blue: Int)

case class Station(stn: Int, wban: Int, lat: Double, lon: Double)

object Station {

  def valid(fields: Array[String]): Boolean = {
    fields.length == 4 && fields(2).nonEmpty && fields(3).nonEmpty
  }

  def parse(fields: Array[String]): Station = {
    Station(
      stn = Try(fields(0).toInt).getOrElse(0),
      wban = Try(fields(1).toInt).getOrElse(0),
      lat = fields(2).toDouble,
      lon = fields(3).toDouble)
  }
}

case class Record(stn: Int, wban: Int, month: Int, day: Int, temp: Double)

object Record {

  implicit class F2C(f: String) {
    def toCelsius: Double = (f.toDouble - 32) * 5 / 9
  }

  def valid(fields: Array[String]): Boolean = {
    fields.length == 5 && fields(2).nonEmpty && fields(3).nonEmpty && fields(4).nonEmpty
  }

  def parse(fields: Array[String]): Record = {
    Record(
      stn = Try(fields(0).toInt).getOrElse(0),
      wban = Try(fields(1).toInt).getOrElse(0),
      month = fields(2).toInt,
      day = fields(3).toInt,
      temp = fields(4).toCelsius)
  }
}

