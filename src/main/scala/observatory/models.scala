package observatory

case class Station(stn: String, wban: String, lat: Double, lon: Double)

case class Record(stn: String, wban: String, month: Int, day: Int, temp: Double)

case class Location(lat: Double, lon: Double)

case class Color(red: Int, green: Int, blue: Int)

