package messaging

import java.util.concurrent.atomic.AtomicLong
import java.io.{InputStream, OutputStream}
import akka.actor.ActorRef

/**
 * Startup message sent to MessagingActor to initiate a new Messaging instantiation.
 *
 * Regular messages will automatically be chunked/de-chunked by the system, so the application doesn't have
 * to worry about the size of a message. Very large messages - e.g. a series of chunks intended to transfer
 * a 4GB file - should be chunked by the Application. The chunks for such large messages are normally also
 * sent through the low-priority mailbox to the outbound Actor to avoid congesting the channel.
 * 
 * Each MessagingActor handles one InputStream and one OutputStream, and these should not be touched outside of
 * that MessagingActor or everything will probably break!
 *
 * Within one MessagingActor, multiple Connection IDs may be defined - so that several logical connections can be
 * multiplexed across the same InputStream/OutputStream. Since each StartConn message also declares the Actor for
 * the Application, different Actors may be specified for each Connection, or the same Actor for all Connections
 * (and the Application must then handle the de-multiplexing), or some combination - e.g. one Application Actor
 * handles N Connections, another Application Actor handles M Connections, etc.
 *
 * Only one msgIDGenerator exists for each MessagingActor, so if several Connections are multiplexed the msgID's
 * within outbound messages will not normally be in sequence for each Connection.
 *
 * Each time an outbound Message is sent, an ACK will be sent to ALL Application actors with it's msgID. Comparing
 * this to the current value of the msgIDGenerator yields the number of outbound messages still in the queue and
 * allows the Application to throttle if necessary.
 *
 * As a CONVENTION, the msgID's generated on the Server side are positive and those on the Client side negative.
 *
 * @param instrm            - the InputStream to be read for inbound messages - each chunk expanded to a Message instance
 * @param outstrm           - the OutputStream where Message instances are written
 * @param msgIDGenerator    - AtomicLong used to generate msgID values for this MessagingActor system.
 */
case class Startup (name:String, instrm:InputStream, outstrm:OutputStream, msgIDGenerator:AtomicLong = new AtomicLong) {
  def toShort = s"Startup[Name: $name, InputStream: ${System.identityHashCode(instrm)}, OutputStream: ${System.identityHashCode(outstrm)}, MsgIDGen: ${System.identityHashCode(msgIDGenerator)}]"
}

/**
 * Once the MessagingActor has been initialized with the Startup message, this message may be sent to define a Connection.
 *
 * @param connID            - unique ID of this connection
 * @param actorApplication  - ref to the Actor which has the actual Application logic - where to send inbound messages
 */
case class StartConn(connID:Long, actorApplication:ActorRef) {
  def toShort = f"StartConn[ConnID: $connID%,d, AppActor: $actorApplication]"
}

/** Message sent to the Application Actor to provide the connection information the Application will need.
 *
 * @param connID            - unique ID of this connection
 * @param msgIDGenerator    - AtomicLong used to assign new msgID values when an outbound Message is created
 * @param actorOutMessaging - where the Application should send outbound Message instances
 */
case class AppConnect(connID:Long, actorOutMessaging:ActorRef, msgIDGenerator:Option[AtomicLong]){
  def toShort = f"Messaging[ConnID: $connID%,d]"
}
/** Message sent to the MessagingActor (and the In and Out Actors) to close down a particular
 *  Connection. If connID == -1, or this is the LAST active connID, then do a complete shutdown
 *  Message is also passed to the Application for that Connection (or ALL Applications)
 **/
case class Close(connID:Long) {
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
 *  Note: This backpressure logic is within the Application, not this Messaging system itself!
 **/
case class ACK(connID:Long, msgID:Long, totalData:Long, amtSent:Long)

/** A NACK for an outbound message -- sent back to the original sender only */
case class NACK(connID:Long, msgID:Long, reason:Int)

object NACK {
  val hashFailed = 1
  val badLength  = 2
  val badBuffer  = 3                // ByteBuffer not valid - must have backing array, be big-endian, at least base size
  val badHash    = 4
  val badOTW     = 5                // Presented OverTheWireExpanded with key fields missing
}

//case class MessagingStarted(messaging:ActorRef, inbound:ActorRef, outbound:ActorRef, app:ActorRef)
