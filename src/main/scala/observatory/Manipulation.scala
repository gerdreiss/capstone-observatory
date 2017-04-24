package observatory

/**
  * 4th milestone: value-added information
  */
object Manipulation {

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Double)]): (Int, Int) => Double = {
    //(x: Int, y: Int) => Visualization.predictTemperature(temperatures, Location(x.toDouble, y.toDouble))
    val grid: Map[Location, Double] = {
      for {
        lat <- -89 to 90
        lon <- -180 to 179
      } yield Location(lat, lon) -> Visualization.predictTemperature(temperatures, Location(lat, lon))
    }.toMap

    (x: Int, y: Int) => grid(Location(x, y))
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Double)]]): (Int, Int) => Double = {
    //(x: Int, y: Int) => {
    //  val ts: Iterable[Double] = temperaturess.par
    //    .map(temps => makeGrid(temps)(x, y))
    //    .seq
    //  ts.sum / ts.size
    //}
    val grids: Iterable[(Int, Int) => Double] = temperaturess.map(makeGrid)

    (x: Int, y: Int) => {
      val ts = grids.map(_(x, y))
      ts.sum / ts.size
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param normals      A grid containing the “normal” temperatures
    * @return A sequence of grids containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Double)], normals: (Int, Int) => Double): (Int, Int) => Double = {
    (x: Int, y: Int) => {
      // Standard deviation
      //math.sqrt(math.pow(makeGrid(temperatures)(x, y) - normals(x, y), 2) / temperatures.size)
      // Just deviation
      makeGrid(temperatures)(x, y) - normals(x, y)
    }
  }
}

