package jdctransport

/**
 * Once the TransportActor has been initialized, this message may be sent to define a Connection.
 *
 * @param connID  - unique ID of this connection or ZERO if not yet assigned
 * @param app     - the Application - where to send inbound messages
 */
final case class StartConn(connID:Int, app:TransportApplication) {
  def toShort = f"StartConn[ConnID: $connID%,d, App: $app]"
}

/** Message sent to the Application Actor to provide the connection information the Application will need.
 *
 * @param connID    - unique ID of this connection -- may still be ZERO in some situations
 * @param trans     - the Transport involved
 */
final case class AppConnect(connID:Int, trans:Transport){
  override def toString= toShort
  def toShort = f"Messaging[ConnID: $connID%,d, ${trans.toShort}]"
}
/** If on the client side, connID is not yet known, so can send this to get things started */
final case class StartClient(app:TransportApplication)


/** ACK message sent to the Message.ackTo after each successful send of a Message (or chunk)
 *
 *  Note: This backpressure logic is within the Application, not this Messaging system itself! If the Application
 *        is sending large data amounts in chunks, then the AppData fields should be populated and used to determine
 *        how many chunks are still in the queue in order to apply backpressure. The number of chunks should be
 *        minimized where possible, since these chunks may delay the sending of other Application messages.
 **/
final case class ACK(connID:Int, msg:TransMessage, appData:Option[AppData]=None, sentAt:Long = System.currentTimeMillis)

/** A NACK for an outbound message, with full Message -- sent back the ackTo */
final case class NAK(connID:Int, failReason:Int, msg:TransMessage)

object NAK {
  val badLength     = 1
  val badBuffer     = 2                // ByteBuffer not valid - must have backing array, be big-endian, at least base size
  val notValid      = 3
  val badOTW        = 4                // Presented OverTheWireExpanded with key fields missing
  val connIDClosed  = 5
}

/** Standard Error message.
 *  May be sent by an Application back to a sender to report an error. Will also be sent by the standard FileTransfer
 *  logic if an I/O error occurs during sending of a file.
 *
 *  The connID and msgID will be the same as the inbound message being answered or the File Transfer msgID
 **/
final case class Error(connID:Int=0, msgID:Long=0, errCode:Int=0, errors:List[String] = Nil){
  def isEmpty   = errCode==0 && errors.isEmpty
  def nonEmpty  = !isEmpty

  override def toString = if(isEmpty) "No Error" else f"ERROR[Conn: $connID%,d, MsgID: $msgID%,d, Code: $errCode, Errs: ${errors.mkString(" -- ")}]"
}
object ErrorCodes {
  val notAllowed  = 1
  val ioError     = 2
}
