package prod

import org.apache.spark.sql.{Dataset, SparkSession}

case class FlightData(
                       passengerId: String,
                       flightId: Int,
                       from: String,
                       to: String,
                       date: String
                     )

case class FlightCount(
                        Month: Short,
                        numberOfFlights: Long
                      )

object q1PureScala extends App {

  val spark = SparkSession.builder()
    .appName("Read data")
    .master("local[*]") // Set the master URL as "local[*]"
    .getOrCreate()

  import spark.implicits._

  val inputFlightDataCsvPath = "src/main/resources/data/flightData.csv"
  val flightCountsPath = "src/main/resources/data/flightsByMonth.csv"

  val flightData = readFlightData(inputFlightDataCsvPath) // Read the flight data from a CSV file

  def readFlightData(inputFlightDataCsvPath: String): Dataset[FlightData] = {
    spark.read
      .options(Map(
        "header" -> "true",
        "sep" -> ",",
        "nullValue" -> ""))
      .csv(inputFlightDataCsvPath)
      .as[FlightData]
  }

  def calculateFlightCounts(flightData: Dataset[FlightData]): Dataset[FlightCount] = {
    flightData
      .map(data => (data.date.split("-")(1).toShort, 1))
      .groupByKey(_._1)
      .count()
      .map { case (month, count) => FlightCount(month, count) }
      .sort("Month")
  }

  def writeFlightCounts(flightCounts: Dataset[FlightCount], flightCountsPath: String): Unit = {
    flightCounts.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv(flightCountsPath)
  }

  val flightCounts = calculateFlightCounts(flightData) // Calculate the total number of flights for each month
  writeFlightCounts(flightCounts, flightCountsPath) // Write the flight counts to a CSV file

  // Display the result
  flightData.show(10, truncate = false)
  flightCounts.show(10, truncate = false)
}

