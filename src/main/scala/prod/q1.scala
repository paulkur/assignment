package prod

import org.apache.spark.sql.{Dataset, SparkSession}

object q1 extends App {

  val inputFlightDataCsvPath = "src/main/resources/data/flightData.csv"
  val flightCountsPath = "src/main/resources/data/exports/flightsByMonth.csv"

  val spark = SparkSession.builder()
    .appName("Read data")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def readFlightData(inputFlightDataCsvPath: String): Dataset[FlightData] = {
    spark.read
      .options(Map(
        "header" -> "true",
        "sep" -> ",",
        "nullValue" -> ""))
      .csv(inputFlightDataCsvPath)
      .select(
        $"passengerId".cast("Int"),
        $"flightId".cast("Int"),
        $"from",
        $"to",
        $"date"
      )
      .as[FlightData]
  }

  def calculateFlightCounts(flightData: Dataset[FlightData]): Dataset[FlightCount] = {
    flightData
      .withColumn("Month", $"date".substr(6, 2).cast("Short"))
      .groupBy($"Month")
      .count()
      .select($"Month", $"count".as("numberOfFlights"))
      .as[FlightCount]
      .sort($"Month") // Sort by Month column in ascending order
  }

  def writeFlightCounts(flightCounts: Dataset[FlightCount], flightCountsPath: String): Unit = {
    flightCounts
      .coalesce(1) // Combine the data into a single partition
      .write
      .mode("overwrite") // Add overwrite mode option
      .options(Map(
        "header" -> "true",
        "sep" -> ",",
        "nullValue" -> ""))
      .csv(flightCountsPath)
  }

  val flightData = readFlightData(inputFlightDataCsvPath) // Read the flight data from a CSV file
  val flightCounts = calculateFlightCounts(flightData) // Calculate the flight counts
  writeFlightCounts(flightCounts, flightCountsPath) // Write the flight counts to a CSV file
  flightCounts.show(15, truncate = false) // Display the result
}
