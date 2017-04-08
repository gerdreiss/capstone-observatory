package observatory

import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

import scala.reflect.ClassTag

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

  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  implicit def tuple3[A1, A2, A3](implicit e1: Encoder[A1], e2: Encoder[A2], e3: Encoder[A3]): Encoder[(A1, A2, A3)] =
    Encoders.tuple[A1, A2, A3](e1, e2, e3)

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {

    readStations(stationsFile).createOrReplaceTempView("stations")
    readTemperatures(temperaturesFile).createOrReplaceTempView("temperatures")

    sparkSession.sql(
      """select s.lat, s.lon, t.month, t.day, t.temp
           from stations s
           join temperatures t on s.stn = t.stn and s.wban = t.wban""")
      .map(row => (
        LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day")),
        Location(row.getAs[Double]("lat"), row.getAs[Double]("lon")),
        row.getAs[Double]("temp")))
      .collect()
  }

  private def readStations(stationsFile: String) = {
    sparkSession.sparkContext.textFile(Extraction.getClass.getResource(stationsFile).toExternalForm)
      .map(_.split(","))
      .filter(Station.valid)
      .map(Station.parse)
      .filter(_.lat != 0.0)
      .filter(_.lon != 0.0)
      .toDF()
  }

  private def readTemperatures(temperaturesFile: String) = {
    sparkSession.sparkContext.textFile(Extraction.getClass.getResource(temperaturesFile).toExternalForm)
      .map(_.split(","))
      .filter(Record.valid)
      .map(Record.parse)
      .filter(_.temp != 9999.9)
      .toDF()
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    records
      // group by year and location
      .groupBy(t => (t._1.getYear, t._2))
      // extract the temperature values from the triplets
      .mapValues(_.map(value => value._3)).toSeq
      // compute the temperature averages
      .map(entry => (entry._1._2, entry._2.sum / entry._2.size))
  }
}
