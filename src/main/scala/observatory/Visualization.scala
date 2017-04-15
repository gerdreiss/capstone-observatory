package observatory

import com.sksamuel.scrimage.{Image, Pixel, RGBColor}

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  private val W = 360
  private val H = 180
  private lazy val IMAGE_XY = for (x <- 0 until W; y <- 0 until H) yield Coord(x, y)

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Double)], location: Location): Double = {
    temperatures.find(_._1 == location)
      .map(_._2)
      .getOrElse({
        //import Spark.session.implicits._
        //Spark.session.sparkContext
        //  .parallelize(temperatures.toSeq)
        //  .map(t => {
        //    val idw = location.idw(t._1)
        //    (t._2 * idw, idw)
        //  })
        //  .toDF("temp", "idw")
        //  .select(sum($"temp").as[Double], sum($"idw").as[Double])
        //  .map(row => row._1 / row._2)
        //  .first()
        val result = temperatures.par
          .map(t => {
            val idw = location.idw(t._1)
            (t._2 * idw, idw)
          })
          .foldLeft[(Double, Double)]((0.0, 0.0))((d1, d2) => (d1._1 + d2._1, d1._2 + d2._2))
        result._1 / result._2
      })
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Double, Color)], value: Double): Color = {
    points.find(_._1 == value)
      .map(_._2)
      .getOrElse({
        points.dropWhile(_._1 < value).toList match {
          case p1 :: p2 :: _ => interpolate(p1, p2, value)
          case             _ => interpolate(points.init.last, points.last, value)
        }
      })
  }

  private def interpolate(p1: (Double, Color), p2: (Double, Color), value: Double): Color = {
    import observatory.implicits._
    Color(
      interpolate(p1._1, p1._2.red,   p2._1, p2._2.red,   value).toRGB,
      interpolate(p1._1, p1._2.green, p2._1, p2._2.green, value).toRGB,
      interpolate(p1._1, p1._2.blue,  p2._1, p2._2.blue,  value).toRGB
    )
  }

  private def interpolate(temp1: Double, rgbp1: Int, temp2: Double, rgbp2: Int, value: Double): Double = {
    rgbp1 + (value - temp1) * (rgbp2 - rgbp1) / (temp2 - temp1)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {
    Image(W, H, pixels(temperatures, colors))
  }

  private def pixels(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Array[Pixel] = {
    //Spark.session.sparkContext.parallelize(IMAGE_XY)
    IMAGE_XY
      .map(coord => predictTemperature(temperatures, Location.fromCoord(coord)))
      .map(temp  => interpolateColor(colors, temp))
      .map(color => Pixel(RGBColor(color.red, color.green, color.blue)))
      .toArray
      //.collect()
  }
}
