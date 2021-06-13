package jdcmessaging

import java.nio._
import java.io.{InputStream, OutputStream}
import java.util.Arrays
import java.util.concurrent.atomic._
import akka.actor.{ActorRef, Actor, Props}

import scala.collection.mutable

/**
TODO
----
-- implement the SSL support
*/
/** This is a low-level messaging system between two applications. More importantly, it can be
 *  routed through an Xchange server so that applications with non-routable IP addresses can both
 *  connect (outbound) to an Xchange server and have messages transparently cross-routed between them.
 *
 *  This Messaging system (and the related Xchange server):
 *  -- a very basic message format
 *  -- SSL support (NYI)
 *  -- transparently have either direct connections or via Xchange
 *     -- only differences are some initial login messages needed to establish the cross-connect thru Xchange
 *        -- if direct connect, participants just ignore these
 *  -- low latency - just real-time message passing, no permanent storage or persistent queueing
 *  -- incorporates both normal messages and file transfers
 *     -- and anything else - only the ultimate sender & receiver know the meaning & format of each message
 *  -- the 'name' of the message is critical
 *     -- must not match one of the 'name's defined in the Messaging object -- see the msg... name constants
 *     -- should be unique within the application
 *     -- must be agreed upon by senders and receivers
 *     -- tells the receiver how to interpret the 'data'
 *        -- e.g. data is JSON, data is raw bytes (file transfer), data is Java serialized object, data is
 *                a combination - e.g. file transfer sends 'chunks' of data, first chunk has the 'name'
 *                as agreed upon and a JSON 'data' section with the name of the target file. All following
 *                chunks contain raw data to write to the file.
 */

/** Overview
 *  --------
 *  -- recipient puts up a ServerSocket and accepts connections from senders
 *  -- upon accepting a new connection assigns a unique connection ID (for the life of the application)
 *  -- creates MessagingActor instance to handle this connection (passes the in/out Socket streams in Messaging message)
 *  -- MessagingActor then
 *     -- creates a child MessagingInActor which
 *        -- reads the inputstream from the socket into buffers
 *          -- will de-chunk messages if requested (as from automatic chunking from MessagingOutActor)
 *        -- creates a Message instance
 *        -- sends this to the application for processing (actorApplication)
 *     -- creates a child MessagingOutActor which
 *        -- handles the OutputStream of the socket
 *        -- receives Message messages from the application
 *          -- will 'chunk' the messages if too long as given in Message
 *        -- converts to buffer form and sends to the outputstream
 *        -- sends an Ack back to the all of the Application actors so that they can implement back-pressure logic
 *
 */

/** Startup Sequence - Server side
 *  ------------------------------
 *  -- puts up a ServerSocket
 *  -- accept() returns with a Socket
 *  -- start the Application Actor
 *  -- assign connID (from global AtomicLong)
 *  -- start Messaging actor - send Startup message to it w name, inputstream, outputstream, msgID Atomic
 *  -- send StartConn message to Messaging to define this ConnID
 *  -- create Message w connID & name = connectionCreated
 *  -- send this to MessagingOutActor so remote end knows the connID
 *
 *  Startup Sequence - Client side
 *  ------------------------------
 *  -- client gets URL & port for the Server (from params, login screen, wherever
 *  -- starts an Application server
 *  -- puts up a Socket(URS, port)
 *  -- start Messaging actor w name, inputstream, outputstream, msgID generator
 *  -- wait for the 'connectionCreated' message from the server
 *  -- send a StartConn message to the Messaging Actor to define the connID
 *  -- start normal processing
 *
 */

/**
 * MessagingActor to initiate a SINGLE new Messaging instantiation.
 *
 * Regular messages will automatically be chunked/de-chunked by the system, so the application doesn't have
 * to worry about the size of a message. Very large messages - e.g. a series of chunks intended to transfer
 * a 4GB file - should be chunked by the Application.
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
 * allows the Application to throttle if necessary. If the Application is sending messages in chunks, then the
 * ACK fields on totalData & amtSent also provide information about how much data remains.
 *
 * As a CONVENTION, the msgID's generated on the Server side are positive and those on the Client side negative.
 *
 * @param name              - Name for this Messaging system. Used only for user messages (debug, info, warn, error)
 * @param instrm            - the InputStream to be read for inbound messages - each chunk expanded to a Message instance
 * @param outstrm           - the OutputStream where Message instances are written
 * @param msgIDGenerator    - AtomicLong used to generate msgID values for this MessagingActor system. Will be provided
 *                            to all Application actors in the AppConnect message.
 */
class MessagingActor(val nameIn:String, val instrm:InputStream, val outstrm:OutputStream, val msgIDGenerator:AtomicLong = new AtomicLong) extends Actor {
  import Messaging._

  val label               = s"Messaging $nameIn"
  val connections         = mutable.Map.empty[Long, ActorRef]     // connID -> application actor

  // ---- Executed at instantiation
  if(bMessagingOpen) debug(s"$label starting -- InputStream: ${System.identityHashCode(instrm)}, OutputStream: ${System.identityHashCode(outstrm)}, MsgIDGen: ${System.identityHashCode(msgIDGenerator)}")
  val inbound = context.actorOf(Props(new MessagingInActor(nameIn, instrm, msgIDGenerator)))
  val outbound= context.actorOf(Props(new MessagingOutActor(nameIn, outstrm, msgIDGenerator)))
  /////////////////////////////////

  def receive = {

    case conn:StartConn   => startConn(label, conn, connections, sendToApp = true, outbound = outbound, msgID = Some(msgIDGenerator))
                             toInAndOut(conn)               // NOTE: May be a dup message

    case msg:Close        => if(bMessagingClose) debug(msg.toShort)
                             if(msg.connID == connIDCloseAll){
                               connections.foreach{ case(connID, ref) => ref ! msg}
                               toInAndOut(msg)
                               context.stop(self)
                             } else connections.get(msg.connID) match {
                                 case None      => warn(f"Close for ConnID: ${msg.connID}%,d, but not found -- ignored")
                                 case Some(ref) => ref ! msg
                                                   toInAndOut(msg)
                               }
                        }

  def toInAndOut(msg:AnyRef) = {
    inbound ! msg
    outbound ! msg
  }
}
// Used by MessagingInActor to keep reading InputStream without a complete block
private case class ReadInitial()     // A read - just get the 'length' field
private case class ReadRemaining()
private case class ReadChunks()

/** Actor to handle the InputStream of the socket. Will send Message messages to the Application
 *  Infinite loop reads the Inputstream, sending messages to 'self' so Actor never blocks.
 **/
class MessagingInActor(nameIn:String, instrm:InputStream, msgID:AtomicLong) extends Actor {
  import Messaging._

  val name        = s"Messaging $nameIn IN"
  val connections = mutable.Map.empty[Long, ActorRef]         // connID -> application actor
  val bfr         = new Array[Byte](Messaging.maxMessageSize) // Allocate & re-use max size buffer
  val bb          = ByteBuffer.wrap(bfr)

  var bigBfr:Array[Byte] = null             // Large buffer allocated if we need to de-chunk, then released

  // Variables while reading a message
  var length             = 0
  var totalData:Long     = 0          // Needed primarily if we are de-chunking a message
  var amtRead            = 0
  var lastSleep          = 0
  var otw:OnTheWireBuffer= _

  if(bMessagingOpen) debug(s"$name starting -- InStrm: ${System.identityHashCode(instrm)}, msgID: ${System.identityHashCode(msgID)}")

  def receive = {
    case conn:StartConn => startConn(s"MessagingIn $name", conn, connections)

    case cls:Close      => instrm.close
                           context.stop(self)

    case _:ReadInitial  =>bb.reset                    // Start reading a new chunk/message
                          if (instrm.available >= szBaseInfo) {
                            instrm.read(bfr, 0, szBaseInfo)
                            otw       = OnTheWireBuffer(bb)
                            length    = otw.length
                            totalData = otw.totalData
                            amtRead   = szBaseInfo
                            // The happy path - not de-chunking and the whole message is ready to be read
                            if(!otw.isDeChunk && instrm.available >= length - szBaseInfo){
                              instrm.read(bfr, szBaseInfo, length - szBaseInfo)
                              sendTo(OnTheWireBuffer(bb).asMessage, otw.connID)
                              reset
                              self ! ReadInitial
                            } else {
                              self ! (if(otw.isDeChunk) ReadChunks else ReadRemaining)
                            }
                          } else {      // No input data available, wait an interval before trying again
                            Thread.sleep(sleepFor)
                            self ! ReadInitial
                          }

    case _:ReadRemaining=>// Read the remaining data for a Message if not a reChunk situation
                          val avail = { val tmp = instrm.available
                                        if (tmp > length - amtRead) length - amtRead else tmp
                                      }
                          if(avail > 0){
                            instrm.read(bfr, amtRead, avail)
                            amtRead += avail
                            if(amtRead == length) {
                              sendTo(OnTheWireBuffer(bb).asMessage, otw.connID)
                              reset
                              self ! ReadInitial
                            } else {
                              lastSleep = 0             // Actually had some data, so reset the sleep to minimum
                              self ! ReadRemaining
                            }
                          } else {
                            Thread.sleep(sleepFor)
                            self ! ReadRemaining
                          }

    case _:ReadChunks   =>// Have to de-chunk the incoming Message - we probably chunked it on the outbound side
                          if(bigBfr==null) {
                            bigBfr = new Array[Byte](totalData.toInt)
                            System.arraycopy(bfr, 0, bigBfr, 0, szBaseInfo) // copy the Base Info read by ReadInitial
                          }
                          val availNow = instrm.available
                          if(availNow > 0){
                            val readAmt = (if(availNow < totalData - amtRead) availNow else totalData - amtRead).toInt
                            instrm.read(bigBfr, amtRead, readAmt)
                            amtRead += readAmt
                            if(amtRead==totalData){   // We're done!
                              val bigBB = ByteBuffer.wrap(bigBfr)
                              sendTo(OnTheWireBuffer(bigBB).asMessage, otw.connID)
                              reset
                              self ! ReadInitial
                            } else {
                              lastSleep = 0
                              self ! ReadChunks
                            }
                          } else {
                            Thread.sleep(sleepFor)
                            self ! ReadChunks
                          }
  }

  def reset = {
    length    = 0
    totalData = 0
    lastSleep = 0
    amtRead   = 0
    otw       = null
    bigBfr    = null
  }
  def sendTo(msg:Message, connID:Long) = {
    if(connID == connIDBroadcast) {
      connections.values.foreach( _ ! msg)
    } else connections.get(connID) match {
      case None       => error(f"$name -- ConnID: $connID%,d is not defined")
      case Some(ref)  => ref ! msg
    }
  }

  def sleepFor = {
    lastSleep = if(lastSleep==0) instrmSleepInitial else if(lastSleep >= instrmSleepMax) instrmSleepMax else lastSleep * 2
    lastSleep
  }
}

/** Application sends output Message's to this Actor, which will chunk them if necessary.
 *  Note: Large data transfers should be chunked by the Application so the whole thing is not in memory!
 *  ACK is sent once each message or chunk is passed to the OutputStream
 */
class MessagingOutActor(val nameIn:String, val outstrm:OutputStream, val msgID:AtomicLong) extends Actor {
  import Messaging._
  val name:String         = s"$nameIn OUT"
  val connections         = mutable.Map.empty[Long, ActorRef]     // connID -> application actor

  if(bMessagingOpen) debug(s"$name starting -- OutStrm: ${System.identityHashCode(outstrm)}, msgID: ${System.identityHashCode(msgID)}")

  def receive = {

    case conn:StartConn => startConn(name, conn, connections)

    case otw:Message    => if(otw.failReason != 0)
                              sender ! NACK(otw.connID, otw.msgID, otw.failReason)
                            else if(!otw.isPopulated)
                              sender ! NACK(otw.connID, otw.msgID, NACK.badOTW)
                            else {
                              if (otw.length > Messaging.maxMessageSize)
                                sendChunks(otw)
                              else {
                                sendOneBuffer(otw)
                              }
                              // Even if we chunked it, to the sender it looks like a single message was sent
                              sender ! ACK(otw.connID, otw.msgID, otw.totalData, otw.totalData)
                              if(otw.ackTo != Actor.noSender)
                                otw.ackTo ! otw
                            }

    case cls:Close     => outstrm.close
                               context.stop(self)
  }

  private def sendOneBuffer(otw:Message) = {
    try {
      outstrm.write(otw.asBuffer)
      if(bMessagingOutbound) debug(s"Sent -- ${otw.strShort}")
    } catch {
      case ex:Exception => error(s"MessagingOutActor for Connection ID: ${otw.connID}, Ex: $ex")
                           context.parent ! Close
    }
  }

  // chunk the message and send directly. Cannot recursively send chunks to ourselves because then
  // chunks may arrive at the receiver interspersed with other application messages. This could be
  // handled, but then the Application could receive messages out of order.
  // Note: Name and data1stChunk are only in the 1st chunk
  private def sendChunks(msg:Message) = {
    if(msg.isChunked) {
      error(f"Connection ID: ${msg.connID}%,d, MsgID: ${msg.msgID}%,d, Name: ${msg.name}, is already chunked but too large: ${msg.length}%,d")
      context.parent ! Close
    } else {
      if(msg.data.length > maxAutoChunk) {
        warn(f"Connection ID: ${msg.connID}%,d, MsgID: ${msg.msgID}%,d, Name: ${msg.name}, is very large: ${msg.data.length}%,d -- should be chunked by the Application")
      }
      val maxData1st = maxData(msg.name.getBytes.length)
      val maxDataNth = maxData(0)
      val totalData  = msg.data.length
      val numChunks  = ((totalData - maxData1st)/maxDataNth) + 1

      var dataOffset = 0
      for(n <- 1 to numChunks){
        val sendMsg = if(n == 1)
                        msg.copy(flags = flagChunked | flagDeChunk, data1stChunk = maxData1st, totalData = totalData, data = Arrays.copyOfRange(msg.data, 0, maxData1st))
                      else {
                        val fullTo = dataOffset + maxDataNth
                        val dataTo = if(fullTo > msg.data.length) msg.data.length else fullTo
                        msg.copy(flags = flagChunked | flagDeChunk, data1stChunk = 0, name="", totalData = totalData, data = Arrays.copyOfRange(msg.data, dataOffset, dataTo))
                      }
        sendOneBuffer(sendMsg)
        dataOffset += sendMsg.data.length
      }
    }
  }
}
object Messaging {
  // INJECT methods for debug, info, warn, error messages - default is just print to the console
  var debug:(String) => Unit = (str:String) => println(s"DEBUG: $str")
  var info:(String)  => Unit  = (str:String) => println(s" INFO: $str")
  var warn:(String)  => Unit  = (str:String) => println(s" WARN: $str")
  var error:(String) => Unit = (str:String) => println(s"ERROR: $str")

  val connIDBroadcast= -2
  val connIDCloseAll = -1
  val connectionID   = new AtomicInteger    // Assignment of ConnectionIDs -- Server-side use only

  val instrmSleepInitial = 8        // If waiting for input data, first sleep interval ... then double it
  val instrmSleepMax     = 128      // ... until exceeds this max wait time

  val maxMessageSize = 8 * 1024     // Larger messages must be 'chunked' into pieces
                                    // Size chosen to both reduce memory pressure and to prevent a large Message
                                    // from bottlenecking the channel. Applications should 'chunk' very large messages
                                    // and send each chunk - so other messages may be interleaved.

  val maxAutoChunk    = 20 * maxMessageSize // Arbitrary, but over this size will clog the output stream

  val flagInbound     = 0x01<<0     // This is an INBOUND message (i.e. it was read from the InputStream)
  val flagChunked     = 0x01<<1     // This message has more than 1 chunk, see OnTheWireChunked
  val flagDeChunk     = 0x01<<2     // Receiver should de-chunk this message before sending it to the application
  val maskOutChunked  = (~(flagChunked | flagDeChunk)) & 0xFF   // Mask with all 'chunked' flags OFF

  val maskByte    = 0x00FF
  val maskInt     = 0x00FFFFFFFFL

  // Size & Offsets, etc to various fields within the on-the-wire buffer
  val szLength          = 4
  val szTotalData       = 8
  val szHash            = 4
  val szFlags           = 1
  val szSzName          = 1
  val szConnID          = 4
  val szMsgID           = 4

  val offsetLength      = 0
  val offsetTotalData   = offsetLength    + szLength
  val offsetHash        = offsetTotalData + szTotalData
  val offsetFlags       = offsetHash      + szHash
  val offsetSzName      = offsetFlags     + szFlags
  val offsetConnID      = offsetSzName    + szSzName
  val offsetMsgID       = offsetConnID    + szConnID
  val offsetName        = offsetMsgID     + szMsgID

  val szBaseInfo        = szLength + szTotalData + szHash + szFlags + szSzName + szConnID + szMsgID

  def offsetData(szName:Int) = offsetName + szName
  def maxData(szName:Int)    = maxMessageSize - offsetData(szName) - szName

  // These messages are handled by the Messaging system itself. There is no security and/or login involved, the
  // application can require login messages (handled by the application) if desired.
  // These names are specialized and should not be used by any other part of the application
  val msgCreateConnection = "##CreateConnection$$"     // User to server-side to open a connection
  val msgConnectionCreated= "##ConnectionCreated$$"    // Connection ID stored as Int at the beginning of data array
  val msgCloseConnection  = "##CloseConnection$$"      // User to server-side, no response, server just closes the socket
  val msgConnectionRefused= "##ConnectionRefused$$"    // Error Code stored as Int at the beginning of data array

  /** One of the actors got a StartConn message - update the given Map in place
   *  and (optionally) send an AppConnect message to the Application
   **/
  def startConn(label:String, conn:StartConn, connections:mutable.Map[Long, ActorRef], sendToApp:Boolean = false, outbound:ActorRef=Actor.noSender, msgID:Option[AtomicLong] = None ) = {
    if (bMessagingOpen) debug(s"$label ${conn.toShort}")
    connections.get(conn.connID) match {
      case None      => connections += (conn.connID -> conn.actorApplication)
      case Some(ref) => if (ref != conn.actorApplication) {    // If ==, have a dup message so just ignore
                          warn(f"$label -- ConnID: ${conn.connID}%,d was already defined -- changing Application reference")
                          connections += (conn.connID -> conn.actorApplication)
                        }
    }
    if(sendToApp) conn.actorApplication ! AppConnect(conn.connID, outbound, msgID)
  }

  // Debug flags - if bMessaging == false, then ALL are disabled
  // NOTE: final val ==> the compiler will not even generate code into the .class file for any
  //       statement of the form if(flag){ .... }.
  //
  //       Change 'final val' to 'var' if you want to change these dynamically at runtime.
  final val bMessaging         = true
  final val bMessagingOpen     = bMessaging && true
  final val bMessagingClose    = bMessaging && true
  final val bMessagingActor    = bMessaging && true
  final val bMessagingOutbound = bMessaging && true
  final val bMessagingInbound  = bMessaging && true
  final val bMessagingDeChunk  = bMessaging && true
}









