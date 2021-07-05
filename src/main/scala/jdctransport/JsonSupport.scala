package jdctransport

import spray.json._

trait JsonSupport extends DefaultJsonProtocol {

  implicit val formatAppData        = jsonFormat4(AppData)
  //implicit val formatMessage        = jsonFormat8(Message)
  implicit val formatXfrInfoFormat  = jsonFormat12(FileTransferInfo)
  implicit val formatError          = jsonFormat4(Error)
}

