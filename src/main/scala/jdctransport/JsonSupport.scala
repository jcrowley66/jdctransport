package jdctransport

import spray.json._
import akka.serialization._

trait JsonSupport extends DefaultJsonProtocol {

  implicit val formatAppData    = jsonFormat4(AppData)
  implicit val formatError      = jsonFormat4(Error)
  implicit val formatFTAppData  = jsonFormat7(FTAppData)
  implicit val formatFTRequest  = jsonFormat8(FTRequest)
  implicit val formatFTInfo     = jsonFormat17(FTInfo)
}

