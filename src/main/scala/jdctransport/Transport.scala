package jdctransport

import java.nio._
import java.nio.channels._
import java.util.ArrayDeque
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.Arrays
import java.util.concurrent.atomic._
import akka.actor.{ActorContext, ActorRef, Actor, Props}

import scala.collection.mutable
import scala.collection.concurrent.TrieMap
/**
TODO
----
-- implement the SSL support
-- consider public-key encryption so each Viewer has their own separate encryption during any data
   transfer from the Publisher
   -- generate public/private keys (from?? Must be standard libraries available.)
   -- at Login, Viewer sends public key as part of User Login
   -- as part of login to the Viewer, user must enter private key
      -- scramble this in memory
   -- for any data transfer,
      -- from Viewer to Publisher use private key to encrypt the message
      -- from Publisher to Viewer use public key
*/
/** This is a low-level messaging system between two applications. More importantly, it can be
 *  routed through an Xchange server so that applications with non-routable IP addresses can both
 *  connect (outbound) to an Xchange server and have messages transparently cross-routed between them.
 *
 *  This Messaging system (and the related Xchange server):
 *  -- a very basic message format
 *  -- SSL support (NYI) or public/private encryption (TBD)
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
 *  -- based on non-blocking ServerChannel's for input/output
 *  -- built on Akka Actors -- hence require non-blocking Channel
 *  -- recipient puts up a ServerSocketChannel and accepts connections from senders
 *  -- upon accepting a new connection assigns a unique connection ID (for the life of the application)
 *  -- creates MessagingActor instance to handle this connection (passes the SocketChannel)
 *  -- MessagingActor then
 *     -- creates a child MessagingInActor which
 *        -- reads the input data from the socket into buffers
 *          -- will de-chunk messages if requested (as from automatic chunking from MessagingOutActor)
 *        -- creates a Message instance
 *        -- sends this to the application for processing (actorApplication)
 *     -- creates a child MessagingOutActor which
 *        -- handles the output data of the socket
 *        -- receives Message messages from the application
 *          -- will 'chunk' the messages if too long as given in Message
 *        -- converts to buffer form and sends to the output channel
 *        -- sends an Ack back to the sending Actor (if ackTo is provided)
 *
 */
/** Backpressure/throttling
 *  -----------------------
 *
 *  Basically,
 *  -- backpressure exists for each separate SocketChannel - which may be supporting many Application Actors and
 *     have many different Connection IDs multiplexed over that channel
 *  -- for each SocketChannel, the identityHashCode is used as a unique ID
 *  -- the 'Transport.inFlight' Map provides an AtomicInteger counter for each unique SocketChannel
 *  -- when a TransportOutActor actually sends a Message to the channel, it DECREMENTs this counter
 *  -- when an Application Actor wants to send a Message, XXXXXX
 *
 *  NOTE: This works only if all Actors are in the same JVM. If Actors are scaled across JVM's and servers, we'll
 *        need to come up with something else!
 **/

/** Startup Sequence - Server side
 *  ------------------------------
 *  -- puts up a ServerSocketChannel
 *  -- accept() returns with a SocketChannel
 *  -- start the Application Actor
 *  -- assign connID (from global AtomicLong)
 *  -- start Messaging actor - send Startup message to it w name, socket channel, msgID Atomic
 *  -- send StartConn message to Messaging to define this ConnID
 *  -- create Message w connID & name = connectionCreated
 *  -- send this to MessagingOutActor so remote end knows the connID
 *
 *  Startup Sequence - Client side
 *  ------------------------------
 *  -- client gets URL & port for the Server (from params, login screen, wherever
 *  -- starts an Application server
 *  -- puts up a SocketChannel(URS, port)
 *  -- start Messaging actor w name, socket channel, msgID generator
 *  -- wait for the 'connectionCreated' message from the server
 *  -- send a StartConn message to the Messaging Actor to define the connID
 *  -- start normal processing
 *
 */

// Used by several Actors to form a state machine so nothing ever blocks.
// Also passed to the SleepFor logic to delay
trait TransportStep         // NOTE: This is used in several different classes
private final case class ReadBaseInfo() extends TransportStep   // A read - just get the base information
private final case class ReadBaseMore() extends TransportStep   // Read the rest of the base information
private final case class ReadRemaining()extends TransportStep
private final case class WriteInitial() extends TransportStep   // Initial Write of a Message
private final case class WriteMore()    extends TransportStep   // ... continue Writing the current Message

private final case class CheckQueue()    extends TransportStep   // Used by Application Actors with the SendMessage trait

/**
 * MessagingActor to initiate a SINGLE new Messaging instantiation.
 *
 * Regular messages will automatically be chunked/de-chunked by the system, so the application doesn't have
 * to worry about the size of a message. Very large messages - e.g. a series of chunks intended to transfer
 * a 4GB file - should be chunked by the Application.
 *
 * Each MessagingActor handles one SocketChannel, and this should not be touched outside of
 * that MessagingActor or everything will probably break!
 *
 * Within one MessagingActor, multiple Connection IDs may be defined - so that several logical connections can be
 * multiplexed across the same InputStream/OutputStream. Since each StartConn message also declares the Actor for
 * the Application, different Actors may be specified for each Connection, or the same Actor for all Connections
 * (and the Application must then handle the de-multiplexing), or some combination - e.g. one Application Actor
 * handles N Connections, another Application Actor handles M Connections, etc.
 *
 * NOTE: MsgID's generated on the SERVER are even-numbered, on the CLIENT odd-numbered. The msgID generator for
 *       a Client is created when the 'msgConnectionCreated' is received during establishment of the connection.
 *
 * @param name    - Name for this transport system. Used only for user messages (debug, info, warn, error)
 * @param channel - the SocketChannel to be handled
 */
class TransportActor(val nameIn:String, val channel:SocketChannel) extends Actor {
  import Transport._

  val channelID   = System.identityHashCode(channel)      // Unique ID for this OutputStream
  val name        = f"TransportPARENT: $nameIn, Channel: $channelID%d"
  val connections = mutable.Map.empty[Long, ActorRef]     // connID -> application actor

  // ---- Executed at instantiation
  if(bTransportOpen) debug(s"$name STARTING $self -- Channel: $channelID")
  if(channel.isBlocking) throw new IllegalStateException(s"$name -- SocketChannel must be in non-blocking mode")
  val outbound= context.actorOf(Props(new TransportOutActor(nameIn, channel)))
  val inbound = context.actorOf(Props(new TransportInActor(nameIn, channel, outbound)))
  /////////////////////////////////

  def receive = {

    case conn:StartConn   =>if(bTransportOpen) debug(s"$name $self -- ${conn.toShort}")
                            startConn(name, conn, connections, sendToApp = true, outbound = outbound, channelID=channelID)
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

/** Actor to handle the input side of the channel. Will send Message messages to the Application
 *  Infinite loop reads the input, sending messages to 'self' so Actor never blocks.
 **/
class TransportInActor(nameIn:String, channel:SocketChannel, outboundActor:ActorRef) extends Actor with DelayFor {
  import Transport._

  implicit val exec = context.dispatcher

  val actorContext= context       // For the SleepFor trait
  val actorSelf   = self

  val channelID   = System.identityHashCode(channel)
  val name        = f"    TransportIN: $nameIn, Channel: $channelID%,d"
  val connections = mutable.Map.empty[Long, ActorRef]         // connID -> application actor
                                                              // NOTE: will have 0 -> appActor created by StartClient

  // Redirect some Messages to a different ActorRef than what is normal for a given ConnID
  // Expected to be low-volume, mainly to redirect File Transfers
  val redirects   = mutable.Map.empty[(Long, Long), ActorRef]

  val bfr         = new Array[Byte](Transport.maxMessageSize) // Allocate & re-use max size buffer for normal messages
  val bb          = ByteBuffer.wrap(bfr)
  val buffer      = bb.asInstanceOf[Buffer]     // See https://stackoverflow.com/questions/61267495/exception-in-thread-main-java-lang-nosuchmethoderror-java-nio-bytebuffer-flip

  var bigBfr:Array[Byte] = null             // Large buffer allocated if we need to de-chunk, then released
  var bigAmtRead         = 0                // How much data is already in the bigBfr

  var readCycleStarted   = false

  var readBaseCycles     = 0

  // Variables while reading one segment - either a complete Message or a Chunk
  var length             = 0

  if(bTransportOpen) debug(f"$name STARTING $self ")

  def receive = {
    case conn:StartConn => startConn(name, conn, connections, sendToApp=false, outbound=outboundActor, channelID=channelID)
                           if(!readCycleStarted)
                             self ! ReadBaseInfo()

    case clt:StartClient=> if(bTransportOutbound) debug(s"$name -- received StartClient")
                           if(!readCycleStarted) {
                             readCycleStarted = true
                             connections += (0L -> clt.appActor)
                             self ! ReadBaseInfo()
                           }

    case cls:Close      => if(cls.connID==connIDCloseAll) {
                              try {channel.close } catch {case _: Exception => /* IGNORED */ }
                              context.stop(self)
                           } else connections.get(cls.connID) match {
                              case None => warn(s"$name -- Close for ${cls.connID}, but not found in map")
                              case Some(ref) => connections -= cls.connID
                           }


      // Several steps possible in the read cycle. First get all the base info, mainly so we have the 'length'
    case r:ReadBaseInfo =>if(bTransportRdCycle) debug(s"$name -- ReadBaseInfo message")
                          readCycleStarted = true
                          // Start reading one segment - a chunk/message. Read at least the base info if available
                          resetAll
                          buffer.position(0)
                          buffer.limit(szBaseInfo)
                          channelRead
                          if(buffer.position() >= szBaseInfo){
                            length = bb.getInt(0)
                            buffer.limit(length)
                            // See if we can immediately read the entire message & avoid another scheduling trip
                            channelRead
                            if(buffer.position() == length){
                              processSegment
                              self ! ReadBaseInfo()
                            } else
                              self ! ReadRemaining()
                          } else
                            self ! ReadBaseMore()                   // Haven't yet read the entire base-info piece

    case r:ReadBaseMore =>if(bTransportRdCycle) {
                            readBaseCycles += 1
                            if(nEmptyReadCycles > 0 && (readBaseCycles % nEmptyReadCycles) == 0)
                              debug(s"$name -- ReadBaseMore Position: ${buffer.position()}, Length: $length ")
                          }
                          val n = channelRead
                          if(length==0 && buffer.position() >= szLength) {
                            length = bb.getInt(0)
                            buffer.limit(length)
                          }
                          if(buffer.position() >= szBaseInfo) {
                              self ! ReadRemaining()
                          } else if(n > 0)
                            self ! ReadBaseMore()
                          else
                            delayFor(ReadBaseMore())


    case r:ReadRemaining=>if(bTransportRdCycle) debug(s"$name -- ReadRemaining Position: ${buffer.position()}, Length: $length ")
                          // Read the remaining data for one Chunk/Message -- IFF any remaining
                          val n = if(buffer.position() < length)
                                    channelRead
                                  else
                                    0
                          if(buffer.position() == length) {
                            processSegment
                            self ! ReadBaseInfo()
                          } else if(n > 0)
                            self ! ReadRemaining()
                          else
                            delayFor(ReadRemaining())

    case unk             => warn(s"$name -- UNKNOWN skipped -- $unk")
  }

  // Set a REDIRECT of a connID, msgID to a different Actor
  def setRedirect(connID:Long, msgID:Long, to:ActorRef) = redirects += ((connID, msgID) -> to)

  def dropRedirect(connID:Long, msgID:Long) = redirects -= ((connID, msgID))

  def channelRead:Int =
    if(length == 0 || buffer.position() < length) {
      try {
        val n = channel.read(bb)
        if(n > 0) delayReset
        n
      } catch {
        case _:Exception => self ! Close(connIDCloseAll)
                            -1
      }
    } else
      0

  // 'bfr' contains a complete Chunk or Message, decide what to do with it
  def processSegment = {
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
  }

  def sendTo(msg:Message) = {
    val connID = msg.connID
    val msgID = msg.msgID
    if (bTransportInbound) debug(s"$name -- INBOUND MSG: ${msg.strShort}")
    if (connID == connIDBroadcast) {
      connections.values.filterNot(_ == 0).foreach(_ ! msg)
    } else redirects.get((connID, msgID)) match {
      case Some(ref) => ref ! msg
      case None      => connections.get(connID) match {
                          case None      => if (msg.name == Transport.msgConnectionCreated) {
                                              connections.get(0L) match {     // Will have a ZERO entry if starting a Client
                                                case None => error(f"msgConnectionCreated received, but no ZERO-entry has been defined")
                                                case Some(ref) =>
                                                  connections -= 0L
                                                  connections += (msg.connID -> ref)
                                                  Transport.registerConnAndChannel(msg.connID, channelID, 1)
                                                  if (Transport.bTransportInbound) debug(s"$name -- sending AppConnect to $ref")
                                                  ref ! AppConnect(msg.connID, outboundActor, channelID)
                                              }
                                           } else
                                              error(f"$name -- ConnID: $connID%,d is not defined")

                          case Some(ref) => ref ! msg
                        }
    }
  }

  def resetAll = {
    length          = 0
    readBaseCycles  = 0
    delayReset
  }
}

/** Application sends output Message's to this Actor, which will chunk them if necessary.
 *  Note: Large data transfers should be chunked by the Application so the whole thing is not in memory!
 *  ACK is sent once each message or chunk is passed to the OutputStream
 */
class TransportOutActor(nameIn:String, channel:SocketChannel ) extends Actor with DelayFor {
  import Transport._

  val actorContext= context       // For the SleepFor trait
  val actorSelf   = self

  val name:String             = s"   TransportOUT: $nameIn"
  val connections             = mutable.Map.empty[Long, ActorRef]     // connID -> application actor
  val channelID               = System.identityHashCode(channel)
  var inFlight:AtomicInteger  = null

  val queue       = new ArrayDeque[Message]()             // Queue of Message's to be sent. Need to keep this
                                                          // since new Message's can arrive while we're still
                                                          // writing earlier ones.

  var nullCycles         = 0          // Count of null write cycles (nothing to write)

  // Variables while writing one segment - either a complete Message or a Chunk
  var message:Message    = null
  var bb:ByteBuffer      = null
  var length             = 0
  var amtWritten         = 0

  if(bTransportOpen) debug(f"$name STARTING $self -- Channel: $channelID%,d")
  self ! WriteInitial()     // To get the process started

  def receive = {

    case conn:StartConn => startConn(name, conn, connections, false, self, channelID)
                           inFlight = Transport.getInFlightForConnID(conn.connID)

    case _:StartClient  => if(bTransportOutbound) debug(s"$name -- received StartClient")
                          // no-op at this time

    case cls:Close     => if(cls.connID==connIDCloseAll) {
                            try {channel.close } catch {case _: Exception => /* IGNORED */ }
                            context.stop(self)
                          } else connections.get(cls.connID) match {
                            case None => warn(s"$name -- Close for ${cls.connID}, but not found in map")
                            case Some(ref) => connections -= cls.connID
                          }

    case otw:Message    =>if(bTransportOutbound) debug(s"$name -- Outbound Message -- ${otw.strShort}")
                          if(inFlight==null) inFlight = getInFlightForConnID(otw.connID)
                          if(!otw.isValid)
                              sender ! NAK(otw.connID, NAK.notValid, otw)
                          else {
                            if (otw.length > Transport.maxMessageSize)
                              sendChunks(otw)
                            else {
                              queue.add(otw)
                            }
                          }

    case w:WriteInitial =>message = queue.pollFirst
                          if(bTransportOutbound) {
                            if(message==null) {
                              nullCycles += 1
                              if(nNullWriteCycles > 0 && (nullCycles % nNullWriteCycles== 0))
                                debug(f"$name - $nullCycles%,d empty WRITE cycles")
                            } else {
                              debug(s"$name - WRITING ${message.strShort}")
                              nullCycles = 0
                            }
                          }
                          if(message==null)
                            delayFor(WriteInitial())
                          else {
                            resetAll
                            length = message.length
                            bb     = ByteBuffer.wrap(message.asBuffer)
                            doTheWrite
                          }
    case w:WriteMore    =>doTheWrite

    case unk           => warn(s"$name -- UNKNOWN skipped -- $unk")
  }

  def doTheWrite = {
    val n = { try {
                channel.write(bb)
              } catch {
                case _:Exception => self ! Close(connIDCloseAll)
                                    0
              }
            }
    amtWritten += n
    if(bTransportOutDtl) debug(f"$name -- doWrite N: $n%,d, AmtWritten: $amtWritten%,d, Length: $length%,d")
    if(amtWritten==length){
      inFlight.decrementAndGet
      if(message.ackTo != Actor.noSender) message.ackTo ! ACK(message.connID, message)
      delayReset
      self ! WriteInitial()
    } else if(n > 0) {
      delayReset
      self ! WriteMore()
    } else
      delayFor(WriteMore())
  }
  // chunk the message and send directly. Cannot recursively send chunks to ourselves because then
  // chunks may arrive at the receiver interspersed with other application messages. This could be
  // handled, but then the Application could receive messages out of order, which we cannot handle.
  // Note: Name only in the 1st chunk
  private def sendChunks(msg:Message) = {
    if(msg.isChunked) {
      error(f"Connection ID: ${msg.connID}%,d, MsgID: ${msg.msgID}%,d, Name: ${msg.name}, is already chunked but too large: ${msg.length}%,d")
      context.parent ! Close(msg.connID)
    } else {
      if(msg.data.length > maxAutoChunk) {
        warn(f"Connection ID: ${msg.connID}%,d, MsgID: ${msg.msgID}%,d, Name: ${msg.name}, is very large: ${msg.data.length}%,d -- should be chunked by the Application")
      }
      val maxData1st  = maxData(msg.name.getBytes.length)
      val maxDataNth  = maxDataIfNoName
      val totalData   = msg.data.length
      val totalLess1st= totalData - maxData1st
      val numChunks  = (totalLess1st/maxDataNth) + 1 +      // + 1 for the 1st chunk
                       (if((totalLess1st % maxDataNth) == 0) 0 else 1) // + 1 for last piece unless an even fit

      var appData    = AppData(totalData = totalData, sentSoFar = maxData1st, numChunks=numChunks, numThisChunk = 1)
      var dataOffset = 0
      inFlight.addAndGet(numChunks - 1)       // ASSUME that the sender added 1 for the overall Message
      for(n <- 1 to numChunks){
        val sendMsg = if(n == 1) {
                        val rslt = msg.copy(flags = flagChunked | flagDeChunk | flagFirstChunk, data = Arrays.copyOfRange(msg.data, 0, maxData1st), appData = Some(appData))
                        queue.add(rslt)
                        rslt
                      } else {
                        val fullTo = dataOffset + maxDataNth
                        val dataTo = if(fullTo > msg.data.length) msg.data.length else fullTo
                        appData    = appData.copy(sentSoFar = dataTo, numThisChunk = n)
                        val rslt = msg.copy(flags = flagChunked | flagDeChunk | (if(n == numChunks) flagLastChunk else 0), name="", data = Arrays.copyOfRange(msg.data, dataOffset, dataTo), appData = Some(appData))
                        queue.add(rslt)
                        rslt
                      }
        if(bTransportDoChunk)debug(f"$name -- DO CHUNK -- Max1st: $maxData1st%,d, MaxNth: $maxDataNth%,d, Total: $totalData%,d, NumChunks: $numChunks ${sendMsg.strShort}")
        dataOffset += sendMsg.data.length
      }
    }
  }
  def resetAll = {
    bb          = null
    length      = 0
    amtWritten  = 0
    delayReset
  }
}

// Should never Thread.sleep within an Actor. This trait sends a message to the Actor after a given (increasing) delay
trait DelayFor {
  import Transport._
  var lastDelay   = 0
  var delayCount  = 0
  var delayTotal  = 0

  def actorContext:ActorContext   // provided by the Actor extending this trait
  def actorSelf:ActorRef
  def name:String

  def delayFor(nextStep:TransportStep) = {
    implicit val ec = actorContext.system.dispatcher
    lastDelay = if(lastDelay==0) actorSleepInitial else if(lastDelay * 2 >= actorSleepMax) actorSleepMax else lastDelay * 2
    delayCount += 1
    delayTotal += lastDelay
    if(nTransportSleep > 0){
      if( (delayCount % nTransportSleep) == 0)
        debug(s"$name -- SLEEP: $delayCount cycles, total millis: $delayTotal")
    }
    actorContext.system.scheduler.scheduleOnce(Duration.create(lastDelay, MILLISECONDS), actorSelf, nextStep)
  }

  def delayReset = {
    lastDelay   = 0
    delayCount  = 0
    delayTotal  = 0
  }
}
/** Trait to send a Message. Applies back-pressure if too many messages are still in-flight.
 *  The user of this trait MUST pick up the CheckBack() message and call 'sendQueued' when received.
 **/
trait SendMessage extends DelayFor {
  def actorContext:ActorContext   // provided by the Actor extending this trait
  def actorSelf:ActorRef
  def outactor:ActorRef
  def connID:Long

  // Logic - when the inFlight count is > maxInFlight, then enter backpressure mode and just queue all messages
  val minInFlight = Transport.defaultBackpressureLow      // 0 == no min, start sending again as soon as below maxInFlight
                                                          // n == start sending when actual in-flight < n
                                                          //      Note: n == 1 means start sending when 0 in-flight
  val maxInFlight = Transport.defaultBackpressureHigh     // 0 == NO backpressure

  lazy val inFlight = Transport.getInFlightForConnID(connID)    // Lazy to make sure connID has been registered first

  var isInBackpressure = false

  // Queue of Message's to be sent. Messages added here if handling back-pressure
  val queue    = new ArrayDeque[Message]()

  def addToQueue(msg:Message)  = queue.add(msg)

  def sendMessage(msg:Message) = {
    addToQueue(msg)
    sendQueued
  }

  def sendQueued:Unit = {
    // Send one message from the Q, and call again to check the next one
    def sendOne = if (!queue.isEmpty) {
                    inFlight.incrementAndGet
                    outactor ! queue.poll
                    delayReset
                    sendQueued
                  }

    // If have messages, check against any backpressure rules & send if OK
    if (!queue.isEmpty) {
      if (maxInFlight <= 0) {              // NO backpressure at all
        sendOne
      } else {
        val nowInFlight = inFlight.get
        if (nowInFlight >= maxInFlight) {
          isInBackpressure = true
          delayFor(CheckQueue())
        } else if (isInBackpressure) {
          if (minInFlight == 0 || nowInFlight < minInFlight) {
            isInBackpressure = false
            sendOne
          } else
            delayFor(CheckQueue())
        } else
          sendOne
      }
    }
  }

  def sendQSize = queue.size
}
object Transport {
  // INJECT methods for debug, info, warn, error messages - default is just print to the console
  var debug:(String) => Unit = (str:String)  => println(s"DEBUG: $str")
  var info:(String)  => Unit  = (str:String) => println(s" INFO: $str")
  var warn:(String)  => Unit  = (str:String) => println(s" WARN: $str")
  var error:(String) => Unit = (str:String)  => println(s"ERROR: $str")

  /******************************************************************************/
  /** Register a new Connection ID:                                             */
  /** -- updates the mapConnToChannel (ID)                                      */
  /** -- creates (if does not yet exist) a Message ID Generator for Connection  */
  /** -- creates (if does not yet exist) an in-flight counter - note that this  */
  /**     counts all Connections multiplexed over the same Channel              */
  /******************************************************************************/
  def registerConnAndChannel(connID:Long, channelID:Int, msgIDStart:Int = 0) = synchronized {
    mapConnToChannel.get(connID) match {
      case None => mapConnToChannel += (connID -> channelID)
      case Some(id) => if (channelID == id)
                          info(f"ConnID: $connID%,d has already registered Channel: $channelID%,d")
                       else {
                          error(f"ConnID: $connID%,d with Channel: $channelID%,d, was already registered for Channel: $channelID%,d")
                          mapConnToChannel += (connID -> channelID)
                       }
    }
    getInFlightForOutstrm(channelID)       // Just to create the in-flight counter
    getMsgIDGenerator(connID, msgIDStart)  // Just to create a separate Message ID Generator for this Connection
  }

  /**************************************************************************/
  /** Map a Connection ID to the ID of the Channel Output Stream over which */
  /** it travels                                                            */
  /**************************************************************************/
  // Map between a ConnID and an Output stream ID
  private val mapConnToChannel = TrieMap.empty[Long, Int]

  private def connIDToOutstream(connID:Long, outstreamID:Int) = mapConnToChannel += (connID -> outstreamID)

  def getChannelForConnID(connID:Long):Option[Int] = mapConnToChannel.get(connID)

  /**************************************************************************/
  /** Map a Connection ID to a Message ID generator for that Connection     */
  /**************************************************************************/
  // Keyed by Connection ID
  private val msgIDGenerator = TrieMap.empty[Long, AtomicLong]

  def getMsgIDGenerator(connID:Long, startAt:Long = 0):AtomicLong = {
    msgIDGenerator.get(connID) match {
      case Some(atomic) => atomic
      case None         => synchronized ( msgIDGenerator.get(connID) match {
                              case Some(atomic) => atomic
                              case None         => val atomic = new AtomicLong(startAt)
                                                   msgIDGenerator.put(connID, atomic)
                                                   atomic
                            }
                          )
    }
  }

  def getNextMsgID(connID:Long) = getMsgIDGenerator(connID).addAndGet(2)

  /******************************************************************************************************/
  /** Map a Connection ID to a counter of the number of in-flight outbound Messages to serve as the     */
  /** basis for back-pressure. Note that through a single Transport, there may be multiple Connections. */
  /**                                                                                                   */
  /** The sendMessage method in the SendMessage trait will apply backpressure as needed and should be   */
  /** used by all Application Actors instead of the normal actor ! msg logic. If a message is chunked,  */
  /** then each chunk will be counted as a separate message.                                            */
  /**                                                                                                   */
  /** This is used since Akka does not provide a mechanism to see how many messages are in the inbound  */
  /** Mailbox for this outbound Actor. Also, multiple Connections and many Application Actors may all be*/
  /** sending Messages through the same outbound Actor, so this gives an application-wide throttling of */
  /** the common output stream.                                                                         */
  /**                                                                                                   */
  /**NOTE: Applications INCREMENT this for every Message (or Chunk) send to the output Actor, the output*/
  /**      Actor DECREMENTs every time a message/chunk is actually written to the output stream. This   */
  /**      allows the Application Actors to determine how many output Messages (Chunks) are in the      */
  /**      queue, and can throttle output as necessary for overall system health & performance.         */
  /******************************************************************************************************/

  private val inFlight = TrieMap.empty[Long, AtomicInteger]

  /** Get the in-flight counter for a particular Outstream ID, or create if necessary */
  private def getInFlightForOutstrm(outstrmID:Long):AtomicInteger =
    inFlight.get(outstrmID) match {
      case Some(atomic) => atomic
      case None         => synchronized( inFlight.get(outstrmID) match {
                                            case Some(atomic) => atomic
                                            case None         => val atomic = new AtomicInteger
                                                                 inFlight.put(outstrmID, atomic)
                                                                 atomic
                                          }
                                       )
    }

  def getInFlightForConnID(connID:Long):AtomicInteger = {
    getChannelForConnID(connID) match {
      case None     => error(s"No OutstreamID defined for ConnID: $connID -- assigning a dummy!!!")
                       getInFlightForOutstrm(Long.MinValue)
      case Some(id) => getInFlightForOutstrm(id)
    }
  }

  val defaultBackpressureLow  = 3         // Defaults used in the SendMessage trait
  val defaultBackpressureHigh = 8

  val defaultFTBackpressureLow  = 2       // Tighter defaults for File Transfer so it does not clog
  val defaultFTBackPressureHigh = 5       //  the output channels and crowd out other messages

  /******************************************************************************************************/
  val sendMinDelay    = 8                 // Minimum delay in milliseconds -- used by DelayFor trait
  val sendMaxDelay    = 128               // Maximum delay in milliseconds

  val connIDBroadcast= -2
  val connIDCloseAll = -1
  val connectionID   = new AtomicInteger    // Assignment of ConnectionIDs -- Server-side use only

  val actorSleepInitial = 8        // If waiting to read/write channel, first sleep interval ... then double it - millis
  val actorSleepMax     = 256      // ... until exceeds this max wait time

  val maxMessageSize    = 8 * 1024  // Larger messages must be 'chunked' into pieces
                                    // Size chosen to both reduce memory pressure and to prevent a large Message
                                    // from bottlenecking the channel. Applications should 'chunk' very large messages
                                    // and send each chunk - so other messages may be interleaved. We will auto-chunk
                                    // large messages if they have not been chunked by the application.
                                    // NOTE: Fatal error to send a message which is larger than this size and also
                                    //       marked as already chunked!

  val maxAutoChunk    = 20 * maxMessageSize // Arbitrary, but over this size will clog the output stream

  val flagError       = 0x01<<0     // This is an ERROR report, the 'data' will contain
  val flagInbound     = 0x01<<1     // This is an INBOUND message (i.e. it was read from the InputStream)
  val flagChunked     = 0x01<<2     // This message has more than 1 chunk
  val flagDeChunk     = 0x01<<3     // Receiver should de-chunk this message before sending it to the application
  val flagFirstChunk  = 0x01<<4     // This is the first chunk
  val flagLastChunk   = 0x01<<5     // This is the last chunk
  val anyChunkFlag    = flagChunked | flagDeChunk | flagFirstChunk | flagLastChunk

  val maskByte      = 0x00FF
  val maskInt       = 0x00FFFFFFFFL

  // Size & Offsets, etc to various fields within the on-the-wire buffer
  val szLength      = 4
  val szConnID      = 4
  val szMsgID       = 4
  val szFlags       = 1
  val szSzName      = 1

  val offsetLength  = 0
  val offsetConnID  = offsetLength  + szLength
  val offsetMsgID   = offsetConnID  + szConnID
  val offsetFlags   = offsetMsgID   + szMsgID
  val offsetSzName  = offsetFlags   + szFlags
  val offsetName    = offsetSzName  + szSzName

  val szBaseInfo    = szLength + szConnID + szMsgID + szFlags + szSzName

  def offsetData(szName:Int) = offsetName + szName
  
  def maxData(szName:Int)    = maxMessageSize - offsetData(szName)
  val maxDataIfNoName        = maxData(0)

  // These messages are handled by the Messaging system itself. There is no security and/or login involved, the
  // application can require login messages (handled by the application) if desired.
  // These names are specialized and should not be used by any other part of the application
  val msgConnectionCreated= "##ConnectionCreated$$"    // Connection ID is in the Message header
  val msgCloseConnection  = "##CloseConnection$$"      // User to server-side, no response, server just closes the socket
  val msgConnectionRefused= "##ConnectionRefused$$"    // Error Code stored as Int at the beginning of data array

  /** One of the actors got a StartConn message - update the given Map in place
   *  and (optionally) send an AppConnect message to the Application
   **/
  def startConn(label:String, conn:StartConn, connections:mutable.Map[Long, ActorRef], sendToApp:Boolean = false, outbound:ActorRef=Actor.noSender, channelID:Int=0) = {
    if (bTransportOpen) debug(s"$label ${conn.toShort}")
    Transport.registerConnAndChannel(conn.connID, channelID)
    connections.get(conn.connID) match {
      case None      => connections += (conn.connID -> conn.actorApplication)
      case Some(ref) => if (ref != conn.actorApplication) {    // If ==, have a dup message so just ignore
                          warn(f"$label -- ConnID: ${conn.connID}%,d was already defined -- changing Application reference")
                          connections += (conn.connID -> conn.actorApplication)
                        }
    }
    if(sendToApp) conn.actorApplication ! AppConnect(conn.connID, outbound, channelID)
  }

  // Debug flags - if bMessaging == false, then ALL are disabled
  // NOTE: final val ==> the compiler will not even generate code into the .class file for any
  //       statement of the form if(flag){ .... }.
  //
  //       Change 'final val' to 'var' if you want to change these dynamically at runtime.
  final val bTransport         = false
  final val bTransportSetup    = bTransport && false
  final val bTransportOpen     = bTransport && false
  final val bTransportClose    = bTransport && false
  final val bTransportActor    = bTransport && false
  final val bTransportOutbound = bTransport && false
  final val bTransportOutDtl   = bTransport && false
  final val bTransportInbound  = bTransport && false
  final val bTransportInDtl    = bTransport && false
  final val bTransportDeChunk  = bTransport && false
  final val bTransportDoChunk  = bTransport && false
  final val bTransportSegment  = bTransport && false
  final val bTransportRdCycle  = bTransport && false
  final val bTransportBfrExpand= bTransport && false

  final val nNullWriteCycles   = if(bTransport) 100 else 0
  final val nEmptyReadCycles   = if(bTransport) 100 else 0
  final val nTransportSleep    = if(bTransport) 100 else 0
}









