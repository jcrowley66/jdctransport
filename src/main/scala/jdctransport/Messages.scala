package jdctransport

import akka.actor.ActorRef

/**
 * Once the TransportActor has been initialized, this message may be sent to define a Connection.
 *
 * @param connID            - unique ID of this connection or ZERO if not yet assigned (
 * @param actorApplication  - ref to the Actor which has the actual Application logic - where to send inbound messages
 */
final case class StartConn(connID:Long, actorApplication:ActorRef) {
  def toShort = f"StartConn[ConnID: $connID%,d, AppActor: $actorApplication]"
}

/** Message sent to the Application Actor to provide the connection information the Application will need.
 *
 * @param connID            - unique ID of this connection -- may still be ZERO in some situations, but can
 *                            still get the ActorRef of the outbound Actor and (sometimes) the msgIDGenerator
 * @param actorOutMessaging - where the Application should send outbound Message instances
 * @param channelID         - ID of the Channel so the Application can use the sendMessage(actorRef, Message, inFlight, throttleOver)
 */
final case class AppConnect(connID:Long, actorOutMessaging:ActorRef, channelID:Int){
  def toShort = f"Messaging[ConnID: $connID%,d, OutMsg: ${System.identityHashCode(actorOutMessaging)}]"
}
/** If on the client side, connID is not yet known, so can send this to get things started */
final case class StartClient(appActor:ActorRef)

/** Message sent to the TransportActor (and the In and Out Actors) to close down a particular
 *  Connection. If connID == -1, or this is the LAST active connID, then do a complete shutdown
 *  Message is also passed to the Application for that Connection (or ALL Applications)
 **/
final case class Close(connID:Long) {
  def toShort = s"Messaging Close ${if(connID == Transport.connIDCloseAll) "ALL Connections" else s"ConnID: $connID"}"
}

/** ACK message sent to the Message.ackTo after each successful send of a Message (or chunk)
 *
 *  Note: This backpressure logic is within the Application, not this Messaging system itself! If the Application
 *        is sending large data amounts in chunks, then the AppData fields should be populated and used to determine
 *        how many chunks are still in the queue in order to apply backpressure. The number of chunks should be
 *        minimized where possible, since these chunks may delay the sending of other Application messages.
 **/
final case class ACK(connID:Long, msg:Message, appData:Option[AppData]=None, sentAt:Long = System.currentTimeMillis)

/** A NACK for an outbound message, with full Message -- sent back to the original sender only */
final case class NAK(connID:Long, failReason:Int, msg:Message)

object NAK {
  val hashFailed    = 1
  val badLength     = 2
  val badBuffer     = 3                // ByteBuffer not valid - must have backing array, be big-endian, at least base size
  val notValid      = 4
  val badOTW        = 5                // Presented OverTheWireExpanded with key fields missing
}

/** Standard Error message.
 *  May be sent by an Application back to a sender to report an error. Will also be sent by the standard FileTransfer
 *  logic if an I/O error occurs during sending of a file.
 *
 *  The msgID will be the same as the inbound message being answered or the File Transfer msgID
 **/
final case class Error(connID:Long, msgID:Long, errCode:Int=0, errors:List[String] = Nil)
