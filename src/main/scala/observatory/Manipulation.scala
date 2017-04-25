package observatory

import scala.collection.parallel.immutable.ParMap

/**
  * 4th milestone: value-added information
  */
object Manipulation {

  private val locations = for (lat <- -89 to 90; lon <- -180 to 179) yield Location(lat, lon)

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Double)]): (Int, Int) => Double = {
    val grid: ParMap[Location, Double] = locations.par
      .map(loc => loc -> Visualization.predictTemperature(temperatures, loc))
      .toMap
    (x: Int, y: Int) => grid(Location(x, y))
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Double)]]): (Int, Int) => Double = {
    val grids: Iterable[(Int, Int) => Double] = temperaturess.map(makeGrid)
    val averages: ParMap[Location, Double] = locations.par
      .map(loc => loc -> grids.aggregate(0.0)(_ + _ (loc.lat.toInt, loc.lon.toInt), _ + _) / grids.size.toDouble)
      .toMap
    (x: Int, y: Int) => averages(Location(x, y))
  }

  /**
    * @param temperatures Known temperatures
    * @param normals      A grid containing the “normal” temperatures
    * @return A sequence of grids containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Double)], normals: (Int, Int) => Double): (Int, Int) => Double = {
    val grid = makeGrid(temperatures)
    (x: Int, y: Int) => {
      // Standard deviation
      //math.sqrt(math.pow(grid(x, y) - normals(x, y), 2) / temperatures.size)
      // Just deviation
      grid(x, y) - normals(x, y)
    }
  }
}

