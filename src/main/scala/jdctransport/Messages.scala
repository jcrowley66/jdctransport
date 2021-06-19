package jdctransport

import java.util.concurrent.atomic.AtomicLong
import java.io.{InputStream, OutputStream}
import akka.actor.ActorRef

/**
 * Once the MessagingActor has been initialized, this message may be sent to define a Connection.
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
 */
final case class AppConnect(connID:Long, actorOutMessaging:ActorRef){
  def toShort = f"Messaging[ConnID: $connID%,d, OutMsg: ${System.identityHashCode(actorOutMessaging)}]"
}
/** If on the client side, connID is not yet known, so can send this to get things started */
final case class StartClient(appActor:ActorRef)

/** Message sent to the MessagingActor (and the In and Out Actors) to close down a particular
 *  Connection. If connID == -1, or this is the LAST active connID, then do a complete shutdown
 *  Message is also passed to the Application for that Connection (or ALL Applications)
 **/
final case class Close(connID:Long) {
  def toShort = s"Messaging Close ${if(connID == Messaging.connIDCloseAll) "ALL Connections" else s"ConnID: $connID"}"
}

/** ACK message sent to ALL existing Application Actors after each successful send of a Message (or chunk)
 *
 *  In the case of chunked messages, sent after each chunk and the totalAmount and amtSent fields
 *  can be used to determine the progress.
 *
 *  The msgID, versus the current value of the msgIDGenerator, indicates the number of outbound
 *  messages still in the outbound mailbox. The Application(s) may then throttle processing
 *  until the queue size is reduced.
 *
 *  Note: This backpressure logic is within the Application, not this Messaging system itself! If the Application
 *        is sending large data amounts in chunks, then the totalData & amtSent fields can be used to determine
 *        how many chunks are still in the queue in order to apply backpressure. The number of chunks should be
 *        minimized where possible, since these chunks may delay the sending of other Application messages.
 **/
final case class ACK(connID:Long, msgID:Long, totalData:Long, amtSent:Long)

/** A NACK for an outbound message -- sent back to the original sender only */
final case class NACK(connID:Long, msg:Message)

object NACK {
  val hashFailed = 1
  val badLength  = 2
  val badBuffer  = 3                // ByteBuffer not valid - must have backing array, be big-endian, at least base size
  val badHash    = 4
  val badOTW     = 5                // Presented OverTheWireExpanded with key fields missing
}
