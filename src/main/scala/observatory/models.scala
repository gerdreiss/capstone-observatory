package observatory

case class Station(stn: Int, wban: Int, lat: Double, lon: Double)

case class Record(stn: Int, wban: Int, month: Int, day: Int, temp: Double)

case class Location(lat: Double, lon: Double)

case class Color(red: Int, green: Int, blue: Int)

