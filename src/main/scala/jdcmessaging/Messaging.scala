package jdcmessaging

import java.nio._
import java.io.{InputStream, OutputStream}
import java.util.Arrays
import java.util.concurrent.atomic._
import akka.actor.{ActorRef, Actor, Props}
import akka.dispatch.{RequiresMessageQueue, UnboundedStablePriorityMailbox}

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

/** Primary Actor for a SINGLE Messaging instantiation - may have multiple Connections declared **/
class MessagingActor extends Actor {
  import Messaging._
  var startup:Startup     = _
  var inbound:ActorRef    = _
  var outbound:ActorRef   = _
  val connections         = mutable.Map.empty[Long, ActorRef]     // connID -> application actor

  def receive = {
    case msg:Startup      => if(bMessagingOpen) debug(s"MessagingActor starting -- ${msg.toShort}")
                             startup = msg
                             inbound = context.actorOf(Props[MessagingInActor])
                             outbound= context.actorOf(Props[MessagingOutActor])
                             toInAndOut(msg)

    case conn:StartConn   => startConn("MessagingActor", conn, connections, sendToApp = true, outbound = outbound, msgID = Some(startup.msgIDGenerator))
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
class MessagingInActor extends Actor {
  import Messaging._

  // Allocate & re-use max size buffer
  val bfr                = new Array[Byte](Messaging.maxMessageSize)
  val bb                 = ByteBuffer.wrap(bfr)

  var bigBfr:Array[Byte] = null             // Large buffer allocated if we need to de-chunk, then released

  // Initialized when 'msg' is received
  var startup:Startup    = null
  var name:String        = _
  var instrm:InputStream = _
  val connections        = mutable.Map.empty[Long, ActorRef]     // connID -> application actor

  // Variables while reading a message
  var length             = 0
  var totalData:Long     = 0          // Needed primarily if we are de-chunking a message
  var amtRead            = 0
  var lastSleep          = 0
  var otw:OnTheWireBuffer= _

  def receive = {
    case m:Startup      => if(startup == null) {
                              startup = m
                              name    = startup.name
                              instrm  = startup.instrm
                              if(bMessagingOpen) debug(s"MessagingInActor $name starting -- ${startup.toShort}")
                           } else
                              warn(s"MessagingInActor $name - duplicate 'Startup' message ignored")

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
      case None       => error(f"Messaging $name -- ConnID: $connID%,d is not defined")
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
class MessagingOutActor extends Actor with RequiresMessageQueue[UnboundedStablePriorityMailbox] {
  import Messaging._
  var startup:Startup     = null
  var name:String         = _
  var outstrm:OutputStream= _
  val connections         = mutable.Map.empty[Long, ActorRef]     // connID -> application actor

  //def xxx = {
  //  new UnboundedStablePriorityMailbox(new java.util.Comparator[MessageBase] {
  //
  //    def compare(o1: MessageBase, o2: MessageBase): Int = 0
  //  }, 100)
  //}
  def receive = {
    case m:Startup      =>if(startup == null) {
                            startup = m
                            name    = startup.name
                            outstrm = startup.outstrm
                            if(bMessagingOpen) debug(s"MessagingOutActor $name starting -- ${startup.toShort}")
                          } else
                            warn(s"MessagingOutActor $name - duplicate 'Startup' message ignored")

    case conn:StartConn => startConn(s"MessagingOut $name", conn, connections)

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
  final val bMessaging         = true
  final val bMessagingOpen     = bMessaging && true
  final val bMessagingClose    = bMessaging && true
  final val bMessagingActor    = bMessaging && true
  final val bMessagingOutbound = bMessaging && true
  final val bMessagingInbound  = bMessaging && true
  final val bMessagingDeChunk  = bMessaging && true
}

// An original or expanded Message -- all of the numeric fields are larger to hold the unsigned number from the ByteBuffer
// The 'asBuffer' method converts to an Array[Byte] form for transmission. See OnTheWireBuffer to parse an inbound buffer and
// produce a Message.
case class Message (
  // 'length', 'marker', and 'szName' are computed below
  flags    :Int     = 0,                      // Bit flags for certain situations - see Messaging.flag... values
  connID   :Long    = 0,                      // ID of the logical Connection to which this message belongs
  msgID    :Long    = 0,
  name     :String  = "",                     // KEY: Defines the type of message. Could be a class name (e.g. sdm.RespGeneric or sdm.UserLogin)
                                              //      The sender & receiver must decide on all of the 'name' values and the format of the 'data'
                                              //      for this a message of this name.
                                              //      NOTE: String.length may NOT be the same as the Array[Byte] length if any non-Ascii
                                              //            characters appear in the name
  totalData:Long    = 0,                      // Total length of all of the 'data' - for either chunked or non-chunked messages
                                              // NOTE: Can legitimately be zero - e.g. name == CloseDown, so name conveys all information
  data:Array[Byte]  = new Array[Byte](0),     // The actual data. For normal messages data.length == length - sizeBaseInfo - name.getBytes.length
                                              // If the data was chunked, data.length == totalData if Messaging deChunked the message.
                                              // Undefined if the Application receives each chunk and handles all logic.

  // These are not represented in the buffer - for use only within the processing logic
  data1stChunk:Int  = 0,                      // For normal messages == totalData, if de-chunked the length of 'data' in the first chunk
                                              // (in case the application handles this differently)
                                              // This is not sent on-the-wire, but established when the first part of a chunked message arrives
  failReason:Int    = 0,                      // Problem with message if > 0, see NACK for reasons

  amtSent:Long      = 0,                      // Total amount of data sent so far, useful if application is chunking a large message
  ackTo:ActorRef    = ActorRef.noSender       // IFF provided, this Message will be sent to that Actor as an ack
) {
  import Messaging._
  def isPopulated  = connID != 0 && msgID != 0 && (name.nonEmpty || isChunked)
  def isChunked    = (flags & Messaging.flagChunked) > 0
  def isDeChunk    = (flags & Messaging.flagDeChunk) > 0
  def szName       = if(name.isEmpty) 0 else name.getBytes.length
  def length       = szBaseInfo + name.getBytes.length + data.length
  def deChunk      = isChunked && (flags & Messaging.flagDeChunk) > 0
  def dataAsString = new String(data)

  def isInbound    = (flags & Messaging.flagInbound) > 0
  def isOutbound   = !isInbound

  def strChunked = if(isChunked) s"Chunked: T, DeChunk: ${if(deChunk) "T" else "F"}," else ""
  def strShort   = f"OTW[Lnth: $length%,d,$strChunked ConnID: $connID%,d, MsgID: $msgID%,d, TotalData: $totalData%,d, DataSz: ${data.length}%,d, Name: $name]"

  def mustBeChunked = length > maxMessageSize

  /** Convert this expanded information into Array[Byte] buffer form, ready to transmit */
  def asBuffer     = {
    if(mustBeChunked)
      throw new IllegalStateException(f"Message Conn: $connID%,d, MsgID: $msgID%,d, Name: $name -- must be chunked, Length: $length%,d > $maxMessageSize%,d")
    if(!isPopulated)
      throw new IllegalStateException(s"Message missing key data -- $strShort")
    val array = new Array[Byte](length)
    val bb    = ByteBuffer.wrap(array)
    bb.putInt(length)
    bb.putLong(totalData)
    bb.putInt(0)            // Hash == 0, reset after computing the actual hash
    bb.put(flags.toByte)
    bb.put(szName.toByte)
    bb.putInt(connID.toInt)
    bb.putInt(msgID.toInt)

    if(szName > 0) bb.put(name.getBytes)
    bb.put(data)

    val hash = Arrays.hashCode(array)         // Compute the hash with hash field == 0, then stuff into buffer
    bb.putInt(offsetHash, hash)
    array
  }
}

/** Wrap a bytebuffer with helper methods.
 *  ByteBuffer MUST be
 *  -- BIG_ENDIAN
 *  -- have a backing array
 *  -- backing array must have a zero offset
 *
 *  @param isInbound - sets as inbound/outbound - defaults to Inbound since this class is parsing a low-lever buffer
 *                     Note: Sender considers this as an Outbound message, Receiver as Inbound
 */
case class OnTheWireBuffer( bb:ByteBuffer, isFirstChunk:Boolean = true, isInboundParam:Boolean = true ){
  import Messaging._
  // All of the 'val's are fields or logic which must always exist in the message
  val array        = bb.array
  val length       = bb.getInt(offsetLength)
  val totalData    = bb.getLong(offsetTotalData)
  val hash         = bb.getInt(offsetHash).toLong & maskInt
  val flags:Int    = (((bb.get(offsetFlags) & ~Messaging.flagInbound) | (if(isInboundParam) Messaging.flagInbound else 0)).toInt & maskByte)
  val nameSize     = bb.get(offsetSzName).toShort & maskByte
  val connID       = bb.getInt(offsetConnID).toLong & maskInt
  val msgID        = bb.getInt(offsetMsgID).toLong & maskInt
  val name         = if(nameSize==0) "" else new String( bb.array, offsetName, nameSize )
  val posData      = szBaseInfo + nameSize
  val dataLength   = (length - posData)
  val isValidLength= length >= szBaseInfo && length <= maxMessageSize       // Length is not obviously screwy
  val isValidBfr   = bb.hasArray && bb.order==ByteOrder.BIG_ENDIAN && bb.limit >= Messaging.szBaseInfo
  val isValidHash  = {  // Set the in-buffer hash field to 0, compute the hash, put the original hash back
                      bb.putInt(offsetHash, 0)
                      val hashW0 = Arrays.hashCode(array)
                      bb.putInt(offsetHash, hash.toInt)
                      hashW0 == hash
                    }
  def isValid      = isValidLength && isValidBfr && isValidHash

  def isInbound    = (flags & Messaging.flagInbound) > 0
  def isOutbound   = !isInbound
  // These 'def's only exist if the data is chunked so compute only if needed
  def isChunked    = (flags & Messaging.flagChunked) > 0
  def notChunked   = !isChunked
  def isDeChunk    = (flags & Messaging.flagDeChunk) > 0

  /** Return the data as an Array[Byte] */
  def data         = Arrays.copyOfRange(bb.array, posData, dataLength)

  def fail         =if(!isValidLength) NACK.badLength
                    else if(!isValidBfr) NACK.badBuffer
                    else if(!isValidHash) NACK.badHash
                    else 0
  /** Expand this buffer & return Message instance */
  def asMessage   = Message(failReason = fail, flags = flags, connID = connID, msgID = msgID, name = name, totalData = totalData, data1stChunk = if(isFirstChunk) dataLength else 0, data = data)
}

/******************************************************************************************************************/
/* Symbolic definition of the on-the-wire format. Actual processing is at the ByteBuffer level                    */
/* and all of the numerics except 'length' & 'totalData' are UNSIGNED values. See Message & OnTheWireBuffer       */
/******************************************************************************************************************/

// NOTE: 'length' does not need to be read as unsigned since it must be <= maxMessageSize
//       'totalData' also is not read as unsigned since it is a Long and inherently large enough
//       If Byte, Short, Int sign bit is ON, ByteBuffer stores as negative, but retrieved as unsigned by masking

// Note that frequently if 'chunked' the 'name' will appear only in the first chunk - to identify to the recipient what type of
// message this is - and then is szName == 0 afterwards. This is an application decision.
// Also, the data in the first chunk may differ from later chunks. For example, for a file transfer may include both
// the name - e.g. FileTransfer - and in 'data' have JSON with the full target path of the file, creation time, etc.
// Then all subsequent chunks have no name, and the 'data' is just bytes to write into the target file.
// The connID/msgID combination will be the mechanism for unique identification of all the related chunks.
case class OnTheWireModel (                      // sz
  length:Int      = 0,                      //  4  - Length of this message/chunk, including this field
  totalData:Long  = 0,                      //  8 -  Total logical size of 'data' - same as data.length in normal messages, larger if chunked
  hash:Int        = 0,                      //  4  - hash of this message buffer computed WITH this field itself == 0
  flags:Byte      = 0,                      //  1  - Bit flags for certain situations - see Messaging.flag... values
  szName:Byte     = 0,                      //  1  - Size of the 'name' array -- 0 == no name
  connID:Int      = 0,                      //  4  - Unique connection ID to allow multiplexing of logical connections
  msgID:Int       = 0,                      //  4  - Unique Message ID Per Connection ID and unique only in EACH DIRECTION (inbound or outbound)
                                            //       since the SENDER assigns the Message ID. Note: Usually monotonic but no guarantee
                                            //       Will be the same value if there are multiple 'chunks' for this message.
  name:Array[Byte]= new Array[Byte](0),     //  n  - KEY: Defines the type of message. Could be a class name (e.g. sdm.RespGeneric or sdm.UserLogin)
                                            //            The sender & receiver must agree on all names and decide the format of the 'data' for this type of message.
                                            //            Automatic chunking sends the name only in the 1st chunk, szName == 0 in all following chunks
  data:Array[Byte]= new Array[Byte](0)      // (length - size of other fields)
){
  throw new IllegalStateException("OnTheWire is for descriptive purposes ONLY, it should never be instantiated. Use OnTheWireBuffer or Message")
}









