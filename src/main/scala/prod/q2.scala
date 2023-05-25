package prod

import prod.model._
import org.apache.spark.sql.{Dataset, SparkSession}

object q2 extends App {

  val inputFlightDataCsvPath = "src/main/resources/data/flightData.csv"
  val inputPassengerCsvPath = "src/main/resources/data/passengers.csv"
  val frequentFlyersPath = "src/main/resources/data/exports/frequentFlyers.csv"

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

  def getMostFrequentFlyers(passengerData: Dataset[PassengerData], flightData: Dataset[FlightData]): Dataset[FrequentFlyers] = {
    import org.apache.spark.sql.functions._

    val frequentFlyers = flightData
      .groupBy($"passengerId")
      .agg(count("*").alias("numberOfFlights"))
      .join(passengerData, Seq("passengerId"))
      .orderBy($"numberOfFlights".desc)
      .limit(100)
      .select(
        $"passengerId",
        $"numberOfFlights",
        $"firstName",
        $"lastName"
      )
      .as[FrequentFlyers]

    frequentFlyers
  }

  def writeFrequentFlyers(frequentFlyers: Dataset[FrequentFlyers], frequentFlyersPath: String): Unit = {
    frequentFlyers.write
      .mode("overwrite") // Add overwrite mode option
      .options(Map(
        "header" -> "true",
        "sep" -> ","))
      .csv(frequentFlyersPath)
  }

  val flightData = readFlightData(inputFlightDataCsvPath)
  val passengerData = readPassengerData(inputPassengerCsvPath)

  val frequentFlyers = getMostFrequentFlyers(passengerData, flightData)
  writeFrequentFlyers(frequentFlyers, frequentFlyersPath)
  frequentFlyers.show(10, truncate = false)
}
