package observatory

import scala.util.Try

case class Location(lat: Double, lon: Double)

case class Color(red: Int, green: Int, blue: Int)

case class Station(stn: Int, wban: Int, lat: Double, lon: Double)

object Station {

  def valid(fields: Array[String]): Boolean = {
    fields.length == 4 && fields(2).nonEmpty && fields(3).nonEmpty
  }

  def parse(fields: Array[String]): Station = {
    Station(
      stn  = Try(fields(0).toInt).getOrElse(0),
      wban = Try(fields(1).toInt).getOrElse(0),
      lat  = fields(2).toDouble,
      lon  = fields(3).toDouble)
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
      stn   = Try(fields(0).toInt).getOrElse(0),
      wban  = Try(fields(1).toInt).getOrElse(0),
      month = fields(2).toInt,
      day   = fields(3).toInt,
      temp  = fields(4).toCelsius)
  }
}

