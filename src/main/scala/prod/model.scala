package prod

object model {
  case class FlightData(
                         passengerId: Int,
                         flightId: Int,
                         from: String,
                         to: String,
                         date: String
                       )

  case class PassengerData(
                            passengerId: Int,
                            firstName: String,
                            lastName: String
                          )

  case class FlightCount(
                          Month: Short,
                          numberOfFlights: Long
                        )

  case class FrequentFlyers(
                             passengerId: String,
                             numberOfFlights: Long,
                             firstName: String,
                             lastName: String
                           )
}
