package prod

import model._
import org.apache.spark.sql.{Dataset, SparkSession}

object q3scala extends App {

  val inputFlightDataCsvPath = "src/main/resources/data/flightData.csv"
  val inputPassengerCsvPath = "src/main/resources/data/passengers.csv"
  val flights3togetherPath = "src/main/resources/data/flights3together.csv"

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

  def readPassengerData(inputPassengerCsvPath: String): Dataset[PassengerData] = {
    spark.read
      .options(Map(
        "header" -> "true",
        "sep" -> ",",
        "nullValue" -> ""))
      .csv(inputPassengerCsvPath)
      .select(
        $"passengerId".cast("Int"),
        $"firstName",
        $"lastName"
      )
      .as[PassengerData]
  }

  val flightData = readFlightData(inputFlightDataCsvPath) // Read the flight data from a CSV file
  val passengerData = readPassengerData(inputPassengerCsvPath) // Read the passenger data from a CSV file

}

