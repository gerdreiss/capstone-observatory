package observatory

import java.time.LocalDate

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractionTest extends FunSuite {

  test("Extraction.locateTemperatures from year 2015") {
    val result: Iterable[(LocalDate, Location, Double)] = Extraction.locateTemperatures(2015, "/stations.csv", "/2015.csv")
    assert(result.nonEmpty)
  }

  test("Extraction.locationYearlyAverageRecords") {
    val location1 = Location(76.500, 25.067)
    val location2 = Location(70.500, 25.067)
    val records: scala.Iterable[(LocalDate, Location, Double)] = Seq(
      (LocalDate.of(2000, 1, 1), location1, 37.5),
      (LocalDate.of(2000, 1, 2), location1, 37.5),
      (LocalDate.of(2000, 1, 3), location1, 37.5),
      (LocalDate.of(2000, 1, 4), location1, 37.5),
      (LocalDate.of(2000, 1, 5), location2, 30.5),
      (LocalDate.of(2000, 1, 6), location1, 37.5),
      (LocalDate.of(2000, 1, 7), location1, 37.5),
      (LocalDate.of(2000, 1, 8), location1, 37.5),
      (LocalDate.of(2000, 1, 9), location2, 30.5),
      (LocalDate.of(2001, 2, 1), location1, 37.5),
      (LocalDate.of(2001, 2, 2), location1, 37.5),
      (LocalDate.of(2001, 2, 3), location2, 30.5),
      (LocalDate.of(2001, 2, 4), location1, 37.5),
      (LocalDate.of(2001, 2, 5), location1, 37.5)
    )

    val yearlyAvg = Extraction.locationYearlyAverageRecords(records)
    assert(yearlyAvg.nonEmpty)
    assertResult(4)(yearlyAvg.size)
    assertResult(37.5)(yearlyAvg.groupBy(_._1)(location1).head._2)
    assertResult(30.5)(yearlyAvg.groupBy(_._1)(location2).head._2)
  }
}