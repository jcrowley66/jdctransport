package jdctransport

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
 * appData fields (if populated) also provide information about how much data remains.
 *
 * As a CONVENTION, the msgID's generated on the Server side are positive and those on the Client side negative.
 *
 * @param name              - Name for this transport system. Used only for user messages (debug, info, warn, error)
 * @param instrm            - the InputStream to be read for inbound messages - each chunk expanded to a Message instance
 * @param outstrm           - the OutputStream where Message instances are written
 */
class TransportActor(val nameIn:String, val instrm:InputStream, val outstrm:OutputStream) extends Actor {
  import Transport._

  val name        = s"TransportPARENT: $nameIn"
  val connections = mutable.Map.empty[Long, ActorRef]     // connID -> application actor

  // ---- Executed at instantiation
  if(bTransportOpen) debug(s"$name STARTING $self -- InputStream: ${System.identityHashCode(instrm)}, OutputStream: ${System.identityHashCode(outstrm)}")
  val outbound= context.actorOf(Props(new TransportOutActor(nameIn, outstrm)))
  val inbound = context.actorOf(Props(new TransportInActor(nameIn, instrm, outbound)))
  /////////////////////////////////

  def receive = {

    case conn:StartConn   =>if(bTransportOpen) debug(s"$name $self -- ${conn.toShort}")
                            startConn(name, conn, connections, sendToApp = true, outbound = outbound)
                            toInAndOut(conn)               // NOTE: May be a dup message

    case s:StartClient    => toInAndOut(s)

    case msg:Close        => if(bTransportClose) debug(msg.toShort)
                             if(msg.connID == connIDCloseAll){
                               connections.foreach{ case(connID, ref) => ref ! msg}
                               toInAndOut(msg)
                               context.stop(self)
                             } else connections.get(msg.connID) match {
                                 case None      => warn(f"Close for ConnID: ${msg.connID}%,d, but not found -- ignored")
                                 case Some(ref) => ref ! msg
                                                   toInAndOut(msg)
                               }
    case unk              => warn(s"$name -- UNKNOWN skipped -- $unk")
  }

  def toInAndOut(msg:AnyRef) = {
    inbound ! msg
    outbound ! msg
  }
}

// Used by TransportInActor to keep reading InputStream without a complete block
final case class ReadInitial()     // A read - just get the 'length' field
final case class ReadRemaining()

/** Actor to handle the InputStream of the socket. Will send Message messages to the Application
 *  Infinite loop reads the Inputstream, sending messages to 'self' so Actor never blocks.
 **/
class TransportInActor(nameIn:String, instrm:InputStream, outboundActor:ActorRef) extends Actor {
  import Transport._

  val name        = s"    TransportIN: $nameIn"
  val connections = mutable.Map.empty[Long, ActorRef]         // connID -> application actor
                                                              // NOTE: will have 0 -> appActor created by StartClient
  val rdInitial   = ReadInitial()                             // Only need 1 instance of each
  val rdRemaining = ReadRemaining()                           // ... of these

  val bfr         = new Array[Byte](Transport.maxMessageSize) // Allocate & re-use max size buffer for normal messages
  val bb          = ByteBuffer.wrap(bfr)

  var bigBfr:Array[Byte] = null             // Large buffer allocated if we need to de-chunk, then released
  var bigAmtRead         = 0                // How much data is already in the bigBfr

  var readCycleStarted   = false

  // Variables while reading one segment - either a complete Message or a Chunk
  var length             = 0
  var amtRead            = 0
  var lastSleep          = 0
  var sleepCount         = 0
  var sleepTotal         = 0

  var otwBase:OnTheWireBuffer= _

  if(bTransportOpen) debug(s"$name STARTING $self -- InStrm: ${System.identityHashCode(instrm)}")

  def receive = {
    case conn:StartConn => startConn(name, conn, connections)
                           if(!readCycleStarted)
                             self ! rdInitial

    case clt:StartClient=> if(bTransportOutbound) debug(s"$name -- received StartClient")
                           if(!readCycleStarted) {
                             readCycleStarted = true
                             connections += (0L -> clt.appActor)
                             self ! rdInitial
                           }

    case cls:Close      => instrm.close
                           context.stop(self)

    case r:ReadInitial  =>if(bTransportRdCycle) debug(s"$name -- got ReadInitial message")
                          readCycleStarted = true
                          // Start reading one segment - a chunk/message. Read at least the base info if available
                          bb.asInstanceOf[Buffer].rewind    // See https://stackoverflow.com/questions/61267495/exception-in-thread-main-java-lang-nosuchmethoderror-java-nio-bytebuffer-flip
                          if (instrm.available >= szBaseInfo) {
                            instrm.read(bfr, 0, szBaseInfo)
                            otwBase   = OnTheWireBuffer(bb)
                            length    = otwBase.length
                            amtRead   = szBaseInfo
                            if(bTransportInDtl) debug(s"$name -- ${otwBase.baseToString}")
                            if(readSegmentOK)
                              processSegment
                            else
                              self ! rdRemaining
                          } else {      // No input data available, wait an interval before trying again
                            sleepFor
                            self ! rdInitial
                          }

    case r:ReadRemaining=>// Read the remaining data for one Chunk/Message
                          if(readSegmentOK)
                            processSegment
                          else {
                            sleepFor
                            self ! rdRemaining
                          }

    case unk             => warn(s"$name -- UNKNOWN skipped -- $unk")
  }

  // Read more data for this segment, return TRUE if read the complete segment
  def readSegmentOK:Boolean = {
    val avail = instrm.available
    if(bTransportSegment) debug(f"$name -- ReadSegmentOK Avail: $avail%,d, Length: $length%,d, AmtRead: $amtRead%,d")
    if(avail == 0)
      false
    else {
      lastSleep = 0             // Restart the sleep cycle since some data was available
      if(avail >= length - amtRead) {
        instrm.read(bfr, amtRead, length - amtRead)
        amtRead = length
        true
      } else {
        instrm.read(bfr, amtRead, avail)
        amtRead += avail
        false
      }
    }
  }
  // 'bfr' contains a complete Chunk or Message, decide what to do with it
  def processSegment = {
    reset         // Reset the size & counters for reading one segment
    val seg = OnTheWireBuffer(bb, isInboundParam = true).asMessage
    if (bTransportSegment) debug(s"$name OTW INBOUND SEGMENT: ${seg.strShort}")
    if(!seg.isDeChunk){         // We don't have to de-chunk, just send the msg to the App
      sendTo(seg)
    } else {
      if(seg.isFirstChunk && seg.isLastChunk){  // Handle aberration - de-chunk & both first & last chunks in one segment
        sendTo(seg)
      } else if(bigBfr==null) {
        bigBfr = new Array[Byte](Transport.maxMessageSize * 12)         // Guess on size, expand if needed
        System.arraycopy(bfr, 0, bigBfr, 0, seg.length) // copy the first segment
        bigAmtRead = seg.length
        if(bTransportDeChunk) debug(f"DECHUNK -- Allocate BigBfr AmtRead: $bigAmtRead%,d, ${seg.strShort}")
      } else {
        val dataLnth = seg.data.length
        if(bigAmtRead + dataLnth > bigBfr.length){        // Expand the buffer if necessary
          val tmp = bigBfr
          bigBfr = new Array[Byte](bigBfr.length * 2)
          System.arraycopy(tmp, 0, bigBfr, 0, bigAmtRead)
          if(bTransportBfrExpand) debug(f"$name -- OTW INBOUND Expand BigBfr from ${tmp.length}%,d to ${bigBfr.length}%,d")
        }
        System.arraycopy(seg.data, 0, bigBfr, bigAmtRead, dataLnth)
        bigAmtRead += dataLnth
        if(bTransportDeChunk) debug(f"DECHUNK AmtRead: $bigAmtRead%,d, DataLnth: $dataLnth%,d, ${seg.strShort}")
        if(seg.isLastChunk){
          val bbBig = ByteBuffer.wrap(bigBfr)
          bbBig.putInt(offsetLength, bigAmtRead)                                  // Set the 'length' to the correct value
          bbBig.put(offsetFlags, (bbBig.get(offsetFlags) & ~anyChunkFlag).toByte) // Clear the 'chunk' flags
          val sendMsg = OnTheWireBuffer(bbBig, isInboundParam = true).asMessage
          if (bTransportDeChunk) debug(s"$name OTW INBOUND LAST CHUNK: ${sendMsg.strShort}")
          sendTo(sendMsg)
          bigBfr = null
        }
      }
    }
    self ! rdInitial
  }

  def reset = {
    length      = 0
    lastSleep   = 0
    sleepCount  = 0
    sleepTotal  = 0
    amtRead     = 0
    otwBase     = null
  }
  def sendTo(msg:Message) = {
    val connID = msg.connID
    if(bTransportInbound) debug(s"$name -- INBOUND MSG: ${msg.strShort}")
    if(connID == connIDBroadcast) {
      connections.values.filterNot( _ == 0).foreach( _ ! msg)
    } else connections.get(connID) match {
      case None       =>if(msg.name==Transport.msgConnectionCreated) {
                          connections.get(0L) match {     // Will have a ZERO entry if starting a Client
                            case None      => error(f"msgConnectionCreated received, but no ZERO-entry has been defined")
                            case Some(ref) =>
                              connections -= 0L
                              connections += (msg.connID -> ref)
                              ref ! AppConnect(msg.connID, outboundActor)
                          }
                        } else
                          error(f"$name -- ConnID: $connID%,d is not defined")
      case Some(ref)  => ref ! msg
    }
  }

  def sleepFor = {
    lastSleep = if(lastSleep==0) instrmSleepInitial else if(lastSleep * 2 >= instrmSleepMax) instrmSleepMax else lastSleep * 2
    sleepCount += 1
    sleepTotal += lastSleep
    if(nTransportSleep > 0){
      if( (sleepCount % nTransportSleep) == 0)
        debug(s"$name -- SLEEP: $sleepCount cycles, total millis: $sleepTotal")
    }
    Thread.sleep(lastSleep)
  }
}

/** Application sends output Message's to this Actor, which will chunk them if necessary.
 *  Note: Large data transfers should be chunked by the Application so the whole thing is not in memory!
 *  ACK is sent once each message or chunk is passed to the OutputStream
 */
class TransportOutActor(val nameIn:String, val outstrm:OutputStream) extends Actor {
  import Transport._
  val name:String         = s"   MessagingOUT: $nameIn"
  val connections         = mutable.Map.empty[Long, ActorRef]     // connID -> application actor

  if(bTransportOpen) debug(s"$name STARTING $self -- OutStrm: ${System.identityHashCode(outstrm)}")

  def receive = {

    case conn:StartConn => startConn(name, conn, connections)

    case _:StartClient  => if(bTransportOutbound) debug(s"$name -- received StartClient")
                          // no-op at this time

    case otw:Message    =>if(bTransportOutbound) debug(s"$name -- Outbound Message -- ${otw.strShort}")
                          if(!otw.isValid)
                              sender ! NACK(otw.connID, NACK.notValid, otw)
                          else {
                            if (otw.length > Transport.maxMessageSize)
                              sendChunks(otw)
                            else {
                              sendOneBuffer(otw)
                            }
                            // Even if we chunked it, to the sender it looks like a single message was sent
                            sender ! ACK(otw.connID, otw.msgID, appData = otw.appData)
                          }

    case cls:Close     => outstrm.close
                          context.stop(self)
    case unk           => warn(s"$name -- UNKNOWN skipped -- $unk")
  }

  private def sendOneBuffer(otw:Message) = {
    try {
      outstrm.write(otw.asBuffer)
      outstrm.flush
      if(bTransportOutbound) debug(s"$name Sent -- ${otw.strShort}")
    } catch {
      case ex:Exception => error(s"MessagingOutActor for Connection ID: ${otw.connID}, Ex: $ex")
                           context.parent ! Close
    }
  }

  // chunk the message and send directly. Cannot recursively send chunks to ourselves because then
  // chunks may arrive at the receiver interspersed with other application messages. This could be
  // handled, but then the Application could receive messages out of order.
  // Note: Name only in the 1st chunk
  private def sendChunks(msg:Message) = {
    if(msg.isChunked) {
      error(f"Connection ID: ${msg.connID}%,d, MsgID: ${msg.msgID}%,d, Name: ${msg.name}, is already chunked but too large: ${msg.length}%,d")
      context.parent ! Close
    } else {
      if(msg.data.length > maxAutoChunk) {
        warn(f"Connection ID: ${msg.connID}%,d, MsgID: ${msg.msgID}%,d, Name: ${msg.name}, is very large: ${msg.data.length}%,d -- should be chunked by the Application")
      }
      val maxData1st  = maxData(msg.name.getBytes.length)
      val maxDataNth  = maxData(0)
      val totalData   = msg.data.length
      val totalLess1st= totalData - maxData1st
      val numChunks  = (totalLess1st/maxDataNth) + 1 +      // + 1 for the 1st chunk
                       (if((totalLess1st % maxDataNth) == 0) 0 else 1) // + 1 for last piece unless an even fit

      var appData    = AppData(totalData = totalData, sentSoFar = maxData1st, numChunks=numChunks, numThisChunk = 1)
      var dataOffset = 0
      for(n <- 1 to numChunks){
        val sendMsg = if(n == 1) {
                        msg.copy(flags = flagChunked | flagDeChunk | flagFirstChunk, data = Arrays.copyOfRange(msg.data, 0, maxData1st), appData = Some(appData))
                      } else {
                        val fullTo = dataOffset + maxDataNth
                        val dataTo = if(fullTo > msg.data.length) msg.data.length else fullTo
                        appData    = appData.copy(sentSoFar = dataTo, numThisChunk = n)
                        msg.copy(flags = flagChunked | flagDeChunk | (if(n == numChunks) flagLastChunk else 0), name="", data = Arrays.copyOfRange(msg.data, dataOffset, dataTo), appData = Some(appData))
                      }
        if(bTransportDoChunk)debug(f"$name -- DO CHUNK -- Max1st: $maxData1st%,d, MaxNth: $maxDataNth%,d, Total: $totalData%,d, NumChunks: $numChunks ${sendMsg.strShort}")
        sendOneBuffer(sendMsg)
        dataOffset += sendMsg.data.length
      }
    }
  }
}
object Transport {
  // INJECT methods for debug, info, warn, error messages - default is just print to the console
  var debug:(String) => Unit = (str:String) => println(s"DEBUG: $str")
  var info:(String)  => Unit  = (str:String) => println(s" INFO: $str")
  var warn:(String)  => Unit  = (str:String) => println(s" WARN: $str")
  var error:(String) => Unit = (str:String) => println(s"ERROR: $str")

  val connIDBroadcast= -2
  val connIDCloseAll = -1
  val connectionID   = new AtomicInteger    // Assignment of ConnectionIDs -- Server-side use only

  val instrmSleepInitial = 8        // If waiting for input data, first sleep interval ... then double it - millis
  val instrmSleepMax     = 128      // ... until exceeds this max wait time

  val maxMessageSize = 8 * 1024     // Larger messages must be 'chunked' into pieces
                                    // Size chosen to both reduce memory pressure and to prevent a large Message
                                    // from bottlenecking the channel. Applications should 'chunk' very large messages
                                    // and send each chunk - so other messages may be interleaved. We will auto-chunk
                                    // large messages if they have not been chunked by the application.
                                    // NOTE: Fatal error to send a message which is larger than this size and also
                                    //       marked as already chunked!

  val maxAutoChunk    = 20 * maxMessageSize // Arbitrary, but over this size will clog the output stream

  val flagInbound     = 0x01<<0     // This is an INBOUND message (i.e. it was read from the InputStream)
  val flagChunked     = 0x01<<1     // This message has more than 1 chunk
  val flagDeChunk     = 0x01<<2     // Receiver should de-chunk this message before sending it to the application
  val flagFirstChunk  = 0x01<<3     // This is the first chunk
  val flagLastChunk   = 0x01<<4     // This is the last chunk
  val anyChunkFlag    = flagChunked | flagDeChunk | flagFirstChunk | flagLastChunk
//  val maskOutChunked  = (~(flagChunked | flagDeChunk | flagFirstChunk | flagLastChunk)) & 0xFF   // Mask with all 'chunked' flags OFF

  val maskByte    = 0x00FF
  val maskInt     = 0x00FFFFFFFFL

  // Size & Offsets, etc to various fields within the on-the-wire buffer
  val szLength          = 4
  val szHash            = 4
  val szFlags           = 1
  val szSzName          = 1
  val szConnID          = 4
  val szMsgID           = 4

  val offsetLength      = 0
  val offsetHash        = offsetLength    + szLength
  val offsetFlags       = offsetHash      + szHash
  val offsetSzName      = offsetFlags     + szFlags
  val offsetConnID      = offsetSzName    + szSzName
  val offsetMsgID       = offsetConnID    + szConnID
  val offsetName        = offsetMsgID     + szMsgID

  val szBaseInfo        = szLength + szHash + szFlags + szSzName + szConnID + szMsgID

  def offsetData(szName:Int) = offsetName + szName
  def maxData(szName:Int)    = maxMessageSize - offsetData(szName)

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
  def startConn(label:String, conn:StartConn, connections:mutable.Map[Long, ActorRef], sendToApp:Boolean = false, outbound:ActorRef=Actor.noSender) = {
    if (bTransportOpen) debug(s"$label ${conn.toShort}")
    connections.get(conn.connID) match {
      case None      => connections += (conn.connID -> conn.actorApplication)
      case Some(ref) => if (ref != conn.actorApplication) {    // If ==, have a dup message so just ignore
                          warn(f"$label -- ConnID: ${conn.connID}%,d was already defined -- changing Application reference")
                          connections += (conn.connID -> conn.actorApplication)
                        }
    }
    if(sendToApp) conn.actorApplication ! AppConnect(conn.connID, outbound)
  }

  // Debug flags - if bMessaging == false, then ALL are disabled
  // NOTE: final val ==> the compiler will not even generate code into the .class file for any
  //       statement of the form if(flag){ .... }.
  //
  //       Change 'final val' to 'var' if you want to change these dynamically at runtime.
  final val bTransport         = true
  final val bTransportSetup    = bTransport && true
  final val bTransportOpen     = bTransport && true
  final val bTransportClose    = bTransport && true
  final val bTransportActor    = bTransport && true
  final val bTransportOutbound = bTransport && true
  final val bTransportInbound  = bTransport && true
  final val bTransportInDtl    = bTransport && false
  final val bTransportDeChunk  = bTransport && false
  final val bTransportDoChunk  = bTransport && false
  final val bTransportSegment  = bTransport && false
  final val bTransportRdCycle  = bTransport && false
  final val bTransportBfrExpand= bTransport && true

  final val nTransportSleep    = 0
}









