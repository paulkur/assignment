package prod

import java.sql.Date
import prod.model._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object extra extends App {

  val inputFlightDataCsvPath = "src/main/resources/data/flightData.csv"
  val inputPassengerCsvPath = "src/main/resources/data/passengers.csv"
  val flightsNtogetherPath = "src/main/resources/data/flightsNtogether.csv"

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

  def getFlightsTogetherCount(flightData: Dataset[FlightData], passengerData: Dataset[PassengerData]): Dataset[(Int, Long, Int)] = {
    val flightsTogether = flightData
      .groupBy($"passengerId", $"flightId")
      .agg(count("*").alias("flights"))
      .groupBy($"passengerId")
      .agg(sum(when($"flights" > 3, 1L).otherwise(0L)).alias("flightCount"))
      .as[(Int, Long)]

    val passengers = passengerData.as[(Int, String, String)]

    flightsTogether.joinWith(passengers, flightsTogether("_1") === passengers("_1"))
      .map { case ((passengerId, flightCount), (_, firstName, lastName)) =>
        (passengerId, flightCount)
      }
      .filter(_._2 > 0)
      .as[(Int, Long, Int)]
      .toDF("Passenger 1 ID", "Number of flights together")
      .as[(Int, Long, Int)]
      .orderBy($"Number of flights together".desc)
  }

  def writeFlightsTogetherCount(flightCount: Dataset[(Int, Long, Int)], outputPath: String): Unit = {
    flightCount
      .withColumn("Number of flights together", $"Number of flights together".cast("Int"))
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)
  }

  def flownTogether(atLeastNTimes: Int): Dataset[(Int, Int, Int, Date, Date)] = {
    val flightData = readFlightData(inputFlightDataCsvPath) // Read the flight data from a CSV file
    val passengerData = readPassengerData(inputPassengerCsvPath) // Read the passenger data from a CSV file

    val flightsTogether = getFlightsTogetherCount(flightData, passengerData)
      .filter($"Number of flights together" > atLeastNTimes)
      .as[(Int, Long, Int)]

    flightData
      .join(flightsTogether, Seq("passengerId"))
      .select(
        flightData("passengerId").as("Passenger 1 ID"),
        flightsTogether("_1").as("Passenger 2 ID"),
        flightsTogether("_3").as("Number of flights together"),
        flightData("from").as("From"),
        flightData("to").as("To"),
        flightData("date").as("Date")
      )
      .as[(Int, Int, Int, String, String, Date)]
      .toDF("Passenger 1 ID", "Passenger 2 ID", "Number of flights together", "From", "To", "Date")
      .as[(Int, Int, Int, Date, Date)]
  }


  val flightData = readFlightData(inputFlightDataCsvPath) // Read the flight data from a CSV file
  val passengerData = readPassengerData(inputPassengerCsvPath) // Read the passenger data from a CSV file

  val flightCount = getFlightsTogetherCount(flightData, passengerData)
  writeFlightsTogetherCount(flightCount, flightsNtogetherPath)
  flightCount.show(10, truncate = false)

  val atLeastNTimes = 3

  val passengersFlownTogether = flownTogether(atLeastNTimes)
  passengersFlownTogether.show(truncate = false)
}
