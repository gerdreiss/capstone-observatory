package observatory

import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * 1st milestone: data extraction
  */
object Extraction {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Use new SparkSession interface in Spark 2.0
  private[observatory] val sparkSession: SparkSession =
    SparkSession.builder
      .appName("Extraction")
      .master("local[*]")
      .getOrCreate()

  // Infer the schema, and register the DataSet as a table.
  import sparkSession.implicits._

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {

    readStations(stationsFile)
    readTemperatures(temperaturesFile)

    sparkSession.sql(
      """select s.lat, s.lon, t.month, t.day, t.temp
           from stations s
           join temperatures t on s.stn = t.stn and s.wban = t.wban""")
      .map(row => (
        // create Date object as there is no encoder for LocalDate
        java.sql.Date.valueOf(LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day"))),
        Location(row.getAs[Double]("lat"), row.getAs[Double]("lon")),
        row.getAs[Double]("temp")))
      .collect()
      // transform java.sql.Date object to LocalDate
      .map(row => (row._1.toLocalDate, row._2, row._3))
  }

  private def readStations(stationsFile: String) = {
    sparkSession.sparkContext.textFile(Extraction.getClass.getResource(stationsFile).toExternalForm)
      .map(_.split(","))
      .filter(_.length == 4)
      .filter(_(2).nonEmpty)
      .filter(_(3).nonEmpty)
      .map(fields => Station(Try(fields(0).toInt).getOrElse(0), Try(fields(1).toInt).getOrElse(0), fields(2).toDouble, fields(3).toDouble))
      .filter(_.lat != 0.0)
      .filter(_.lon != 0.0)
      .toDF().createOrReplaceTempView("stations")
  }

  private def readTemperatures(temperaturesFile: String) = {
    sparkSession.sparkContext.textFile(Extraction.getClass.getResource(temperaturesFile).toExternalForm)
      .map(_.split(","))
      .filter(_.length == 5)
      .filter(_(2).nonEmpty)
      .filter(_(3).nonEmpty)
      .filter(_(4).nonEmpty)
      .map(fields => Record(Try(fields(0).toInt).getOrElse(0), Try(fields(1).toInt).getOrElse(0), fields(2).toInt, fields(3).toInt, fields(4).toDouble))
      .filter(_.temp != 9999.9)
      .toDF().createOrReplaceTempView("temperatures")
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    records
      // group by year and location
      .groupBy(t => (t._1.getYear, t._2))
      .mapValues(_.map(value => value._3)).toSeq
      .map(entry => (entry._1._2, entry._2.sum / entry._2.size))
  }
}
