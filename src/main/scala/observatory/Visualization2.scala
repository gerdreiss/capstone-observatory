package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Interaction.tileLocation

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {

  private val side: Int = 256
  private val alpha: Int = 127

  /**
    * @param x   X coordinate between 0 and 1
    * @param y   Y coordinate between 0 and 1
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    x: Double,
    y: Double,
    d00: Double,
    d01: Double,
    d10: Double,
    d11: Double
  ): Double = {
    // If we choose a coordinate system
    // in which the four points where f is known
    // are (0, 0), (0, 1), (1, 0), and (1, 1)
    // then the interpolation formula simplifies to
    // f(x, y) ~ f(0, 0)(1 - x)(1 - y) + f(1, 0)x(1 - y) + f(0, 1)(1 - x)y + f(1, 1)xy
    d00 * (1 - x) * (1 - y) + d10 * x * (1 - y) + d01 * (1 - x) * y + d11 * x * y
  }

  /**
    * @param grid   Grid to visualize
    * @param colors Color scale to use
    * @param zoom   Zoom level of the tile to visualize
    * @param x      X value of the tile to visualize
    * @param y      Y value of the tile to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: (Int, Int) => Double,
    colors: Iterable[(Double, Color)],
    zoom: Int,
    x: Int,
    y: Int
  ): Image = {
    Image(side, side, pixels(grid, colors, zoom, x, y))
  }

  private def pixels(grid: (Int, Int) => Double, colors: Iterable[(Double, Color)], zoom: Int, x: Int, y: Int) = {
    (0 until (side * side)).par
      .map(index => tileLocation(zoom, (index % side) / side + x, (index / side) / side + y))
      .map(location => {
        val x1 = location.lon - location.lon.floor.toInt
        val y1 = location.lat.ceil.toInt - location.lat
        val d00 = grid(location.lat.ceil.toInt, location.lon.floor.toInt)
        val d01 = grid(location.lat.floor.toInt, location.lon.floor.toInt)
        val d10 = grid(location.lat.ceil.toInt, location.lon.ceil.toInt)
        val d11 = grid(location.lat.floor.toInt, location.lon.ceil.toInt)
        bilinearInterpolation(x1, y1, d00, d01, d10, d11)
      })
      .map(temperature => Visualization.interpolateColor(colors, temperature))
      .map(color => Pixel(color.red, color.green, color.blue, alpha))
      .toArray
  }
}
