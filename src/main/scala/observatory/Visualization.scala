package observatory

import com.sksamuel.scrimage.Image
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  // Use new SparkSession interface in Spark 2.0
  private[observatory] val sparkSession: SparkSession =
    SparkSession.builder
      .appName("Visualization")
      .master("local[*]")
      .getOrCreate()

  import sparkSession.implicits._

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Double)], location: Location): Double = {
    temperatures.find(_._1 == location)
      .map(_._2)
      .getOrElse({
        sparkSession.sparkContext
          .parallelize(temperatures.toSeq)
          .map(temp => {
            val idw = location.idw(temp._1)
            (temp._2 * idw, idw)
          })
          .toDF("temp", "idw")
          .select(sum($"temp").as[Double], sum($"idw").as[Double])
          .map(row => row._1 / row._2)
          .first()
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
        sparkSession.sparkContext
          .parallelize(points.toSeq)
          .sortByKey()

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

  private def interpolate(temp1: Double, rgbp1: Int, temp2: Double, rgbp2: Int, value: Double) = {
    rgbp1 + (value - temp1) * (rgbp2 - rgbp1) / (temp2 - temp1)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {
    ???
  }

}

