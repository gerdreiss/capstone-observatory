package observatory

import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * 1st milestone: data extraction
  */
object Extraction {

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val sparkSession = SparkSession
      .builder
      .appName("Extraction")
      .master("local[*]")
      .getOrCreate()

    // Infer the schema, and register the DataSet as a table.
    import sparkSession.implicits._

    // Read each line of input data
    val stations = sparkSession.sparkContext.textFile(stationsFile)
      .map(_.split(","))
      .filter(_.length == 4)
      .filter(_.forall(_.nonEmpty))
      .map(fields => Station(fields(0), fields(1), fields(2).toDouble, fields(3).toDouble))
      .filter(_.lat != 0.0)
      .filter(_.lon != 0.0)
      .toDS() //.cache()

    val temperatures = sparkSession.sparkContext.textFile(temperaturesFile)
      .map(_.split(","))
      .filter(_.length == 5)
      .filter(_.forall(_.nonEmpty))
      .map(fields => Record(fields(0), fields(1), fields(2).toInt, fields(3).toInt, fields(4).toDouble))
      .filter(_.temp != 9999.9)
      .toDS() //.cache()

    stations.createOrReplaceTempView("stations")
    temperatures.createOrReplaceTempView("temperatures")

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
