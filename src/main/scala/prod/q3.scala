package prod

import scala.io.Source
import java.io.PrintWriter

object q3 extends App {
  println("Passenger ID\tLongest Run")
  val bufferedSource = Source.fromFile("src/main/resources/data/flightData.csv")
  val lines = bufferedSource.getLines().toList
  bufferedSource.close()

  val passengerData = lines.tail.map(_.split(",").map(_.trim))
  val passengerGroups = passengerData.groupBy(_.head)

  val longestRuns = passengerGroups.map {
    case (passengerId, flights) =>
      val countriesVisited = flights.map(row => row(2)).distinct
      val nonUKRuns = countriesVisited.mkString(" ").split("UK").filterNot(_.contains("UK"))
      val longestRun = if (nonUKRuns.isEmpty) 0 else nonUKRuns.map(_.split(" ").length).max
      (passengerId.toInt, longestRun)
  }.toList

  val longestRunsPath = "src/main/resources/data/exports/longestRunners.csv"

  val sortedLongestRuns = longestRuns.sortBy(-_._2)

  val writer = new PrintWriter(longestRunsPath)
  writer.println("Passenger ID, Longest Run")

  sortedLongestRuns.take(100).foreach { case (passengerId, longestRun) =>
    writer.println(s"$passengerId, $longestRun")
    println(f"$passengerId%-15s\t$longestRun")
  }

  writer.close()
}

