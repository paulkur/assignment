package prod

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
object q4 extends App {

  case class FlightData(
                         passengerId: String,
                         flightId: String,
                         from: String,
                         to: String,
                         date: String
                       )
  case class PassengerData(
                            passengerId: String,
                            firstName: String,
                            lastName: String
                          )
  case class FlightsTogether(
                              passenger1Id: String,
                              passenger2Id: String,
                              numberOfFlightsTogether: Long
                            )

  val inputFlightDataCsvPath = "src/main/resources/data/flightData.csv"
  val inputPassengerCsvPath = "src/main/resources/data/passengers.csv"
  val flights3togetherPath = "src/main/resources/data/exports/flights3together.csv"

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
      .as[FlightData]
  }
  def readPassengerData(inputPassengerCsvPath: String): Dataset[PassengerData] = {
    spark.read
      .options(Map(
        "header" -> "true",
        "sep" -> ",",
        "nullValue" -> ""))
      .csv(inputPassengerCsvPath)
      .as[PassengerData]
  }

  // Calculate the number of flights for each pair of passengers
  def getFlightsTogetherCount(flightData: Dataset[FlightData], passengersData: Dataset[PassengerData]): Dataset[FlightsTogether] = {
    val flightCount = flightData
      .join(passengersData, Seq("passengerId"), "inner")
      .groupBy("flightId")
      .agg(collect_set("passengerId").as("passengerIds"))
      .flatMap { row =>
        val passengerIds = row.getAs[Seq[String]]("passengerIds")
        passengerIds.combinations(2).map(ids => (ids.head, ids(1)))
      }
      .groupBy("_1", "_2")
      .agg(count("*").as("flightCount"))
      .filter($"flightCount" > 3) // Filter for more than 3 flights together
      .select(
        $"_1".alias("passenger1Id"),
        $"_2".alias("passenger2Id"),
        $"flightCount".alias("numberOfFlightsTogether")
      )
      .as[FlightsTogether]

    flightCount
  }

  def writeFlightsTogetherCount(frequentFlyers: Dataset[FlightsTogether], flights3togetherPath: String): Unit = {
    frequentFlyers.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv(flights3togetherPath)
  }

  val flightData = readFlightData(inputFlightDataCsvPath) // Read the flight data from a CSV file
  val passengerData = readPassengerData(inputPassengerCsvPath)

  val flightCount = getFlightsTogetherCount(flightData, passengerData)
  writeFlightsTogetherCount(flightCount, flights3togetherPath)
  flightCount.show(10, truncate = false)
}
