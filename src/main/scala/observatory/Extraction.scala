package observatory

import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

/**
  * 1st milestone: data extraction
  */
object Extraction {

  // Infer the schema, and register the DataSet as a table.
  import Spark.session.implicits._
  import observatory.implicits._

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {

    val stations = readStations(stationsFile)             //.createOrReplaceTempView("stations")
    val temperatures = readTemperatures(temperaturesFile) //.createOrReplaceTempView("temperatures")

    //Spark.session.sql(
    //  """select s.lat, s.lon, t.month, t.day, t.temp
    //       from stations s
    //       join temperatures t on s.stn = t.stn and s.wban = t.wban""")
    //  .map(row => (
    //    LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day")),
    //    Location(row.getAs[Double]("lat"), row.getAs[Double]("lon")),
    //    row.getAs[Double]("temp")))
    //  .collect()

    stations.join(temperatures,
        stations("stn").eqNullSafe(temperatures("stn")) &&
        stations("wban").eqNullSafe(temperatures("wban")))
      .map(row => (
        LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day")),
        Location(row.getAs[Double]("lat"), row.getAs[Double]("lon")),
        row.getAs[Double]("temp").toCelsius
      ))
      .collect()
  }

  private def readStations(stationsFile: String) = {
    //Spark.session.sparkContext
    //  .textFile(Extraction.getClass.getResource(stationsFile).toExternalForm)
    //  .map(_.split(","))
    //  .filter(Station.valid)
    //  .map(Station.parse)
    //  .filter(_.lat != 0.0)
    //  .filter(_.lon != 0.0)
    //  .toDF()
    Spark.session.read
      .option("header", value = false)
      .option("mode", "FAILFAST")
      .schema(Station.structType)
      .csv(Extraction.getClass.getResource(stationsFile).toExternalForm).as[Station]
      .filter((station: Station) => station.lat.isDefined && station.lon.isDefined)
  }

  private def readTemperatures(temperaturesFile: String) = {
    //Spark.session.sparkContext
    //  .textFile(Extraction.getClass.getResource(temperaturesFile).toExternalForm)
    //  .map(_.split(","))
    //  .filter(Record.valid)
    //  .map(Record.parse)
    //  .filter(_.temp != 9999.9)
    //  .toDF()
    Spark.session.read
      .option("header", value = false)
      .option("mode", "FAILFAST")
      .schema(Record.structType)
      .csv(Extraction.getClass.getResource(temperaturesFile).toExternalForm).as[Record]
      .filter((record: Record) => record.temp != 9999.9)
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    // Purely collection based solution
    //records
    //  // group by year and location
    //  .groupBy(t => (t._1.getYear, t._2))
    //  // extract the temperature values from the triplets
    //  .mapValues(_.map(value => value._3)).toSeq
    //  // compute the temperature averages
    //  .map(entry => (entry._1._2, entry._2.sum / entry._2.size))

    // We better use the power of Spark!
    Spark.session.sparkContext
      .parallelize(records.toSeq)
      .map(r => (r._1.getYear, r._2, r._3))
      .toDF("year", "loc", "temp")
      .groupBy($"year", $"loc")
      .agg($"year", $"loc", avg($"temp").as("temp"))
      .select($"loc".as[Location], $"temp".as[Double])
      .collect()
  }
}
