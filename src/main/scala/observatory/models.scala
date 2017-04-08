package observatory

case class Station(stnId: String, wbanId: String, lat: Double, lon: Double)

case class Record(stnId: String, wbanId: String, month: Int, day: Int, temp: Double)

case class Location(lat: Double, lon: Double)

case class Color(red: Int, green: Int, blue: Int)

