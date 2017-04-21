package observatory

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSuite, Inspectors, Matchers}

@RunWith(classOf[JUnitRunner])
class VisualizationTest extends FunSuite with Matchers with TableDrivenPropertyChecks {

  test("predictTemperature") {
    val avg: Iterable[(Location, Double)] =
      Extraction.locationYearlyAverageRecords(Extraction.locateTemperatures(2015, "/stations.csv", "/2015.csv"))
    val temp: Double = Visualization.predictTemperature(avg, Location(90, -180))
    println(temp)
  }

  test("fromCoord") {
    val baseWidth = 360
    val baseHeight = 180
    val testData = Table(
      ("x", "y", "expectedLocation"),
      //img top left
      (0, 0, Location(baseHeight / 2, -baseWidth / 2)),
      //top right
      (baseWidth, 0, Location(baseHeight / 2, baseWidth / 2)),
      //img center
      (baseWidth / 2, baseHeight / 2, Location(0, 0)),
      //img bottom left
      (0, baseHeight, Location(-baseHeight / 2, -baseWidth / 2)),
      //img right bottom
      (baseWidth, baseHeight, Location(-baseHeight / 2, baseWidth / 2))
    )

    Inspectors.forEvery(testData) {
      case (x: Int, y: Int, expectedLocation: Location) =>
        Location.fromPixelIndex(x * y) shouldBe expectedLocation
    }
  }
}