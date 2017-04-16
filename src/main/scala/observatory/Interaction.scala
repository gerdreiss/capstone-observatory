package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Visualization.{interpolateColor, predictTemperature}

import scala.math._

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {

  /**
    * @param zoom Zoom level
    * @param x    X coordinate
    * @param y    Y coordinate
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(zoom: Int, x: Int, y: Int): Location = {
    val p = 1 << zoom
    Location(
      toDegrees(atan(sinh(Pi * (1.0 - 2.0 * y.toDouble / p)))),
      x.toDouble / p * 360.0 - 180.0
    )
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @param zoom         Zoom level
    * @param x            X coordinate
    * @param y            Y coordinate
    * @return A 256×256 image showing the contents of the tile defined by `x`, `y` and `zooms`
    */
  def tile(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)], zoom: Int, x: Int, y: Int): Image = {
    Image(256, 256, pixels(temperatures, colors, zoom, x, y))
  }

  private def pixels(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)], zoom: Int, x: Int, y: Int): Array[Pixel] = {
    (0 until (256 * 256)).par
      .map(index    => tileLocation(zoom, (index % 256) / 256 + x, (index / 256) / 256 + y))
      .map(location => predictTemperature(temperatures, location))
      .map(temp     => interpolateColor(colors, temp))
      .map(color    => Pixel(color.red, color.green, color.blue, 127))
      .toArray
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    *
    * @param yearlyData    Sequence of (year, data), where `data` is some data associated with
    *                      `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](yearlyData: Iterable[(Int, Data)], generateImage: (Int, Int, Int, Int, Data) => Unit): Unit = {
    for {
      (year, data) <- yearlyData
      zoom <- 0 to 3
      x <- 0 until 1 << zoom
      y <- 0 until 1 << zoom
    } generateImage(year, zoom, x, y, data)
  }
}
