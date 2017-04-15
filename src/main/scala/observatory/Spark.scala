package observatory

import org.apache.spark.sql.SparkSession

object Spark {

  private[observatory] lazy val session: SparkSession =
    SparkSession.builder
      .appName("Observatory")
      .master("local[*]")
      .getOrCreate()
}
