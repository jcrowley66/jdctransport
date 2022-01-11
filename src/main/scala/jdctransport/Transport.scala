package jdctransport

import java.nio.file._
import java.nio.channels._
import java.util.ArrayDeque
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.atomic._
import akka.actor.{Props, ActorContext, Actor, ActorSystem, ActorRef}
import jdctransport.Transport.{defaultBackpressureLow, defaultFTBackpressureLow, defaultBackpressureHigh, defaultFTBackPressureHigh}

import scala.collection.mutable
import scala.collection.concurrent.TrieMap

/** Trait must be supported by the Application. Required methods must be supplied by the Application.
 *  Optional methods may be overridden if desired, otherwise default to no-ops */
trait TransportApplication {
  /** REQUIRED -- Message has been received (note, may be part of a file transfer, see transSuppressFileTransfer)
    *             The application should dispatch this to the correct handling method or Actor **/
  def transMsgReceived(msg:TMessage, tran:Transport)
  /** Optional -- Message passed to transMsgSend(..) has physically been sent (Note: passed to the outbound socket, it
    *             may not yet have been received by the target).  **/
  def transMsgSent(msg:TMessage, trans:Transport):Unit = {}
  /** Optional -- New ConnID has been established */
  def transConnOpen(connID:Int, trans:Transport):Unit = {}
  /** Optional -- but recommended. Notification that Transport has closed processing for this ConnID **/
  def transConnClose(connID:Int, trans:Transport):Unit = {}

  /********************************************************************************************************************/
  /** File Transfer operations - all are OPTIONAL if no File Transfers are initiated -- i.e. transferFile(..) called  */
  /**                                                                                                                 */
  /** Note: If any method returns an Option[FTAppData] that is non-empty, that instance will replace the current      */
  /**       instance within the FTInfo instance.                                                                      */
  /********************************************************************************************************************/
  /** Optional -- MUST override to allow File Transfers -- determine if this File Transfer is supported by the Application.  */
  def transAllowFT(info:FTInfo):(Boolean, Option[FTAppData]) = (false, None)
  /** Optional -- terminate the transfer of data -- usually for an isGrowing=true file */
  def transStopTransfer(info:FTInfo):Unit = {}
  /** Optional -- whether to send or suppress File Transfer TMessages to transMsgReceived & transMsgSent. Normally suppressed.
   *              Note: Transport wide suppression since at the point of call there is no way to determine which FT this might be. **/
  def transSuppressFileTransferToMsg:Boolean = true
  /** REQUIRED IFF an outbound file transfer occurs, return full path of the file to read & transfer
   *  If None is returned for the Path, file transfer is aborted.  **/
  def transFTOutboundPath(trans:Transport, info:FTInfo):(Option[Path], Option[FTAppData]) = (None, None)
  /** REQUIRED IFF an inbound file transfer occurs, return full path where file should be written
    *  If None is returned for Path, file transfer is aborted.  **/
  def transFTInboundPath(trans:Transport, info:FTInfo):(Option[Path], Option[FTAppData]) = (None, None)
  /** Optional - Signals that the outbound file transfer is complete. Note: Check 'info' to determine if any error. **/
  def transFTOutboundDone(trans:Transport, info:FTInfo):Unit = {}
  /** Optional - but recommended. Signals that an inbound file transfer is complete and file has
    * been written to the Path returned by transFTInboundPath. Note: Check 'info' to determine if any error. **/
  def transFTInboundDone(trans:Transport, info:FTInfo):Unit = {}
  /** Optional -- Invoked during the stages of a file transfer - see values in object FileInfo. **/
  def transFTStages(trans:Transport, info:FTInfo):Option[FTAppData] = None
}
/** This is a low-level messaging system between two applications. More importantly, it can be
 *  routed through an Xchange server so that applications with non-routable IP addresses can both
 *  connect (outbound) to an Xchange server and have messages transparently cross-routed between them.
 *
 *  This Transport system (and the related Xchange server):
 *  -- a very basic message format
 *  -- transparently have either direct connections or via Xchange
 *     -- only differences are some initial login messages needed to establish the cross-connect thru Xchange
 *        -- if direct connect, participants just ignore these
 *  -- low latency - just real-time message passing, no permanent storage or persistent queueing
 *  -- incorporates both normal messages and file transfers
 *     -- and anything else - only the ultimate sender & receiver know the meaning & format of each message
 *  -- the 'name' of the message is critical
 *     -- must not match one of the 'name's defined in the Transport object -- see the Transport.transConnection... name constants
 *     -- should be unique within the application
 *     -- must be agreed upon by senders and receivers
 *     -- tells the receiver how to interpret the 'data'
 *        -- e.g. data is JSON, data is raw bytes (file transfer), data is Java serialized object, data is
 *                a combination - e.g. file transfer sends 'chunks' of data, first chunk has the 'name'
 *                as agreed upon and a JSON 'data' section with the name of the target file. All following
 *                chunks contain raw data to write to the file.
 *  -- ASSUMPTION - that this Transport operates over a base network (e.g. TCP/IP) which ensures detection and
 *                  retransmission of garbled messages. If this assumption does not hold, then a hash could be
 *                  added to Transport.
 */

/** Overview
 *  --------
 *  -- based on non-blocking ServerChannel's for input/output
 *  -- built on Akka Actors -- hence require non-blocking Channel
 *  -- recipient puts up a ServerSocketChannel and accepts connections from senders
 *  -- upon accepting a new connection assigns a unique connection ID (for the life of the application)
 *  -- creates Transport instance to handle this connection (passes the SocketChannel)
 *  -- Transport then
 *     -- creates a child TransportInActor which
 *        -- reads the input data from the socket into buffers
 *          -- will de-chunk messages if requested (as from automatic chunking from TransportOutActor)
 *        -- creates a Message instance
 *        -- sends this to the application for processing (actorApplication)
 *     -- creates a child TransportOutActor which
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
 *  -- the 'Transport.inFlight' is an AtomicInteger counter for this Transport
 *  -- when a TransportOutActor actually sends a Message to the channel, it DECREMENTs this counter
 *  -- when an Application Actor wants to send a Message, it extends the SendMessage trait which will
 *     increment the counter and handle backpressure restrictions
 *
 *  NOTE: This works only if all Actors are in the same JVM. If Actors are scaled across JVM's and servers, we'll
 *        need to come up with something else!
 **/

/** Startup Sequence - Server side
 *  ------------------------------
 *  -- puts up a ServerSocketChannel
 *  -- accept() returns with a SocketChannel
 *  -- start the Application
 *  -- assign connID
 *  -- create Transport instance - send Startup message to it
 *  -- create Message w connID & name = connectionCreated
 *  -- send this to TransportOutActor so remote end knows the connID
 *
 *  Startup Sequence - Client side
 *  ------------------------------
 *  -- client gets URL & port for the Server (from params, login screen, wherever ...)
 *  -- starts an Application server
 *  -- puts up a SocketChannel(URS, port)
 *  -- instantiate Transport w name, socket channel
 *  -- wait for the 'connectionCreated' message from the server
 *  -- send a StartConn message to Transport to define the connID
 *  -- start normal processing
 *
 */

// Used by several Actors to form a state machine so nothing ever blocks.
// Also passed to the SleepFor logic to delay & SendMessage traits
sealed trait TransportStep         // NOTE: This is used in several different classes
private final case class ReadBaseInfo() extends TransportStep   // A read - just get the base information
private final case class ReadBaseMore() extends TransportStep   // Read the rest of the base information
private final case class ReadRemaining()extends TransportStep
private final case class WriteInitial() extends TransportStep   // Initial Write of a Message
private final case class WriteMore()    extends TransportStep   // ... continue Writing the current Message

private final case class CheckQueue()    extends TransportStep  // Used by Application Actors with the SendMessage trait
private final case class FTXfrN()        extends TransportStep  // Used by File Transfer actors
private final case class FTClear()       extends TransportStep

// FIXME - Transport should really extend from SendMessageTrait in order to implement backpressure
/**
 * One Transport handles a single SocketChannel, and this should not be touched outside of
 * that Transport or everything will probably break!
 *
 * Regular messages will automatically be chunked/de-chunked by the system, so the application doesn't have
 * to worry about the size of a message. Very large messages - e.g. a series of chunks intended to transfer
 * a 4GB file - should be chunked by the Application.
 *
 * Within one Transport, multiple Connection IDs may be defined - so that several logical connections can be
 * multiplexed across the same SocketChannel. The transMsgReceived method is called when each inbound message is
 * complete (it may be re-assembled from multiple chunks), and the Application should dispatch it to the correct
 * method or Actor for processing. Note that multiple Connection IDs may be multiplexed within a single Transport
 * (and associated Socket) if desired by the Application. The Application should then handle dispatching to the
 * correct target.
 *
 * NOTE: MsgID's generated on the SERVER are even-numbered, on the CLIENT odd-numbered. The msgID generator for
 *       a Client is created when the 'msgConnectionCreated' is received during establishment of the connection.
 *
 * @param name      - Name for this transport system. Used only for user messages (debug, info, warn, error)
 * @param channel   - the SocketChannel to be handled
 * @param app       - callback methods to the Application NOTE: Sometimes set to NULL initially because need to
 *                    create both the Application and Transport and we have a chicken-and-egg problem. So set
 *                    this to NULL, then immediately inject the correct TransportApplication.
 * @param maxOutCnt - Maximum queued outbound TMessages - if exceeded, 'sendMessage' will BLOCK
 *                    NOTE: If a long message is broken into N chunks, will count as N messages here
 * @param maxOutData- Maximum queued outbound data (total length) of TMessages - if exceeded, 'sendMessage' will BLOCK
 *
 * @param backPressureHigh    - If the number in flight exceeds this value, then queue all new send requests. 0==infinity - NO backpressure
 * @param backPressureLow     - ... until the number in flight falls below this number
 * @param FTBackPressureHigh  - Different high/low backpressure limits for File Transfers so that a large FT does not crowd
 * @param FTBackPressureLow   - ... out the transfer of normal messages. Normally lower limits for both high & low
 * @param actorOpt  - caller may provide the ActorSystem. If none, then one is created for this Transport
 */
class Transport(val nameIn:String, val channel:SocketChannel, var app:TransportApplication, val actorOpt:Option[ActorSystem]=None,
                val maxOutCnt:Int=Int.MaxValue, val maxOutData:Long=Long.MaxValue,
                val backPressureHigh:Int = defaultBackpressureHigh, val backPressureLow:Int = defaultBackpressureLow,
                val FTBackPressureHigh: Int = defaultFTBackPressureHigh, val FTBackPressureLow:Int = defaultFTBackpressureLow
) {
  import Transport._

  val transID     = assignTransID                 // Unique ID (within a single JVM) for this Transport, must be within 16 bits (unsigned)

  private var userAssignedID = 0
  /** User can assign a user-specific ID, but only once (to make sure it stays unique) */
  def getUserID           = userAssignedID
  def setUserID(id:Int)   = synchronized {
    if(userAssignedID == 0 ) userAssignedID = id
    else                     throw new IllegalStateException(s"assignUserID can only be called once!")
  }

  val channelID   = assignChannelID(channel)                      // Unique ID for this Channel within this JVM
  def name        = f"Transport: $nameIn, UserID: $userAssignedID, Channel: $channelID%d"
  val connections = mutable.Map.empty[Int, TransportApplication]  // connID -> application
                                                                  // NOTE: will have 0 -> app created by StartClient

  val actorSystem = actorOpt.getOrElse(ActorSystem.create(s"Transport_${nameIn}_$channelID"))

  // Redirect some TMessages to an ActorRef other than the normal transMsgReceived call
  // Expected to be low-volume, mainly to redirect File Transfers
  //                                 msgKey -> ActorRef
  val redirects   = new TrieMap[Long, ActorRef]

  /** Redirect a message if it is in the re-direct table */
  def wasReDirected(msg:TMessage):Boolean = {
    if(bTransportRedirect){
      debug(s"Redirect Contents -- ${redirects.keySet.mkString(", ")}")
      debug(s"${if(redirects.get(msg.msgKey).isEmpty) " No" else "YES"} Redirect Msg -- ${msg.strShort}")
    }
    redirects.get(msg.msgKey) match {
      case Some(actor) => actor ! msg; true
      case None        => false
    }
  }
  // Set a REDIRECT given a FTInfo to a different Actor
  def setRedirect(info:FTInfo, to:ActorRef):Unit = setRedirect(info.request.connID, info.xfrMsgID, to)
  // Set a REDIRECT of a connID, msgID to a different Actor
  def setRedirect(connID:Int, msgID:Long, to:ActorRef):Unit = {
    redirects += (((connID.toLong << 32) | msgID) -> to)
    if(bTransportRedirect) debug(s"ReDirect set ConnID: $connID, MsgID: $msgID, ActorRef: $to")
  }
  // Drop a REDIRECT given a FTInfo
  def dropRedirect(info:FTInfo):Unit = dropRedirect(info.request.connID, info.xfrMsgID)
  // Drop a REDIRECT
  def dropRedirect(connID:Int, msgID:Long):Unit = {
    if(bTransportRedirect){
      val ref = redirects.get((connID << 32) | msgID)
      debug(s"Remove Redirect -- ConnID: $connID, MsgID: $msgID, ActorRef: $ref")
    }
    redirects -= ((connID.toLong << 32) | msgID)
  }

  /** For each Transport, have a FileTransferDiscard actor to toss away spurious messages.
   *  (Must be for each Transport. If one system-wide, then if a given Transport shuts down, the
   *  ActorSystem will also shut down, and if the discard actor was associated with that Transport
   *  then it will also shut down.
   **/
  private var discardActor:ActorRef = Actor.noSender

  def getFTDiscard:ActorRef =
    if(discardActor != Actor.noSender)
      discardActor
    else synchronized {
      if(discardActor != Actor.noSender)      // Double-check locking
        discardActor
      else {
        discardActor = actorSystem.actorOf(Props( new FileTransferDiscard(this)))
        discardActor
      }
    }

  // ---- Executed at instantiation
  if(bTransportOpen) debug(s"$name STARTING -- Channel: $channelID")
  if(channel.isBlocking) throw new IllegalStateException(s"$name -- SocketChannel must be in non-blocking mode")
  val outbound= actorSystem.actorOf(Props(new TransportOutActor(this)))
  val inbound = actorSystem.actorOf(Props(new TransportInActor(this)))
  /////////////////////////////////

  val outboundCnt = new AtomicInteger     // Total outbound messages (so apps can throttle if needed)
                                          // Application (or SendMessage trait) bumps, outbound actor decrements
  val outboundData= new AtomicLong()      // Total data (length of all TMessages)
  val inFlight    = new AtomicInteger()   // Count of # passed to TransportOutActor - used for backpressure

  def sendMessageWillBlock = outboundCnt.get > maxOutCnt || outboundData.get > maxOutData
  /** Send a Message -- will block if max in-flight count or data is already exceeded */
  def sendMessage(msg:TMessage):Unit ={
                                        while(sendMessageWillBlock) threadSleep(50)
                                        outboundCnt.incrementAndGet             // Adjusted if msg must be auto-chunked
                                        outboundData.addAndGet(msg.length)      // ditto
                                        outbound ! msg
                                      }
  /** Execute a File Transfer - Note: the transFTInboundPath and transFTInboundDone must be provided by the Application
   *                                  on the inbound side of this transfer.
   *  @return empty if OK, else an error message
   **/
  def transferFile(infoIn:FTInfo):String = {  val (bool, appDataOpt) = app.transAllowFT(infoIn)
                                              val info = updateInfo(infoIn, appDataOpt)
                                              if(bool){
                                                if(info.isInbound) actorSystem.actorOf(Props( new FileTransferIn(this, info)))
                                                else               actorSystem.actorOf(Props( new FileTransferOut(this, info)))
                                                ""
                                              } else
                                                "transAllowFT returned a False"
                                            }
  def start(connID:Int, msgID:Long, app:TransportApplication) = {
    val conn = StartConn(connID, app)
    startConn(name, conn, this, sendToApp = true)
    toInAndOut(conn)
    // Send msg back to the connecting task with the connID assigned
    val msg = TMessage(trans = this, connID = connID, msgID = msgID, name = Transport.transConnectionCreated)
    sendMessage(msg)
  }

  def close(connID:Int) = {
    if (bTransportClose) debug(s"$name -- Closing ConnID: $connID")
    val cls = Close(connID)
    if (connID == connIDCloseAll) {
      connections.foreach { case (connID, app) => app.transConnClose(connID, this) }
      connections.clear
      actorSystem.stop(inbound)
      actorSystem.stop(outbound)
    } else connections.get(connID) match {
      case None      => warn(f"Close for ConnID: ${connID}%,d, but not found -- ignored")
      case Some(app) => app.transConnClose(connID, this); connections -= connID
    }
  }

  def toInAndOut(msg:AnyRef) = {
    inbound ! msg
    outbound ! msg
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
trait SendMessageTrait extends DelayFor {
  // provided by the Actor extending this trait
  def actorContext:ActorContext
  def actorSelf:ActorRef
  def trans:Transport

  // Logic - when the inFlight count is > maxInFlight, then enter backpressure mode and just queue all messages
  def minInFlight:Int     // 0 == no min, start sending again as soon as below maxInFlight
                          // n == start sending when actual in-flight < n
                          //      Note: n == 1 means start sending when 0 in-flight
  def maxInFlight:Int     // 0 == NO backpressure

  private var isInBackpressure = false

  // Queue of Message's to be sent. Messages added here if handling back-pressure
  private val queue    = new ArrayDeque[TMessage]()

  private def addToQueue(msg:TMessage)  = queue.add(msg)

  protected def internalSendMessage(msg:TMessage) = {
    addToQueue(msg)
    internalSendQueued
  }

  protected def internalSendQueued:Unit = {
    // Send one message from the Q, and call again to check the next one
    def sendOne = if (!queue.isEmpty) {
                          val msg = queue.poll
                          trans.outbound ! msg
                          delayReset
                          internalSendQueued
                        }

    // If have messages, check against any backpressure rules & send if OK
    if (!queue.isEmpty) {
      if (maxInFlight <= 0) {              // NO backpressure at all
        sendOne
      } else {
        val nowInFlight = trans.inFlight.get
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

  private val transID = new AtomicInteger

  def assignTransID = transID.incrementAndGet

  /** If one of the app calls returned a new FTAppData, update the FTInfo instance */
  def updateInfo(info:FTInfo, appDataOpt:Option[FTAppData]):FTInfo = if(appDataOpt.isEmpty) info else info.copy(appData = appDataOpt.get)

  /** Assign a simple ID to a channel instead of the full identity hash code */
  private val mapChannelID    = mutable.Map.empty[Int, Int]
  private val channelIDAtomic = new AtomicInteger
  // Assign a simple Channel ID within this JVM (just to not use identityHashCode directly)
  def assignChannelID(channel:SocketChannel) = {
    val ident = System.identityHashCode(channel)
    mapChannelID.get(ident) match {
      case None       => synchronized{
                            val id = channelIDAtomic.incrementAndGet
                            mapChannelID += (ident -> id)
                            id
                         }
      case Some(id) => id
    }
  }

  /** Thread.sleep but just returns on an InterruptedException */
  def threadSleep(millis:Long) =  try{
                                    Thread.sleep(millis)
                                  } catch {
                                    case _:InterruptedException => // no-op
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

  val maxAutoChunk    = 20 * maxMessageSize // Arbitrary, but over this size will clog the output stream. Warning issued.

  /** Test if ONE flag is ON -- WARNING: Throws if 'thisFlag' has >1 flag turned on (may be 0), use 'flagsOn' or 'flagsAny' */
  def flagOn(flags:Long, thisFlag:Long):Boolean   =
    java.lang.Long.bitCount(thisFlag) match {
      case 0 => false
      case 1 => flagsAny(flags, thisFlag)
      case n => throw new IllegalArgumentException(s"jdcscala.flagOn must be 0 or 1 flag - got $thisFlag")
                false
    }

  /** Test if ALL of these flags are ON */
  def flagsOn(flags:Long, testFlags:Long):Boolean = (flags & testFlags) == testFlags
  /** Test if ANY of the test flags is ON -- testFlags may also be a SINGLE flag */
  def flagsAny(flags:Long, testFlags:Long):Boolean= (flags & testFlags) > 0

  val maxSzName     = 1023

  val szNameShift   = 32 - 10           // Shift right to get szName to low-order bits
  val maskSzName    = 0x000003FF        // Mask to pick up the szName (after shift)
  val maskFlags     = 0x00CFFFFF        // Mask to pick up the Flags
  val maskByte      = 0x00FF
  val maskShort     = 0x0000FFFF
  val maskInt       = 0x00FFFFFFFFL

  // Size & Offsets, etc to various fields within the on-the-wire buffer
  val szLength      = 4
  val szTransID     = 2
  val szConnID      = 2
  val szNameFlags   = 4
  val szMsgID       = 4
  val szAppID       = 4

  val offsetLength  = 0
  val offsetTransID = offsetLength  + szLength
  val offsetConnID  = offsetTransID + szTransID
  val offsetSzNFlags= offsetConnID  + szConnID
  val offsetMsgID   = offsetSzNFlags+ szNameFlags
  val offsetAppID   = offsetMsgID   + szMsgID
  val offsetName    = offsetAppID   + szAppID

  val szBaseInfo    = szLength + szTransID + szConnID + szNameFlags + szMsgID + szAppID

  // Offset to the 'data' array given the size of the 'name' field (may be zero)
  def offsetData(szName:Int) = offsetName + szName

  // Max allowed size of 'data' array given the size of the 'name' field (may be zero)
  def maxData(szName:Int)    = maxMessageSize - offsetData(szName)
  // Max allowed 'data' array if the 'name' field is empty (size of 0)
  val maxDataIfNoName        = maxData(0)

  // These messages are handled by the Transport system itself. There is no security and/or login involved, the
  // application can require login messages (handled by the application) if desired.
  // These names are specialized and should not be used by any other part of the application
  val transConnectionCreated  = "##ConnectionCreated$$"    // Connection ID is in the Message header
  val transFTStart            = "##FileTransferStart$$"
  val transFTReady            = "##FileTransferReady$$"
  val transFTStop             = "##FileTransferStop$$"
  val transFTRefused          = "##FileTransferRefused$$"

  /** One of the actors got a StartConn message - update the given Map in place
   *  and (optionally) send an AppConnect message to the Application
   **/
  def startConn(label:String, conn:StartConn, trans:Transport, sendToApp:Boolean = false) = {
    if (bTransportOpen) debug(s"$label ${conn.toShort}")
    trans.connections.get(conn.connID) match {
      case None      => trans.connections += (conn.connID -> conn.app)
      case Some(ref) => if (ref != conn.app) {    // If ==, have a dup message so just ignore
                          warn(f"$label -- ConnID: ${conn.connID}%,d was already defined -- changing Application reference")
                          trans.connections += (conn.connID -> conn.app)
                        }
    }
    if(sendToApp) conn.app.transConnOpen(conn.connID, trans)
  }

  // Debug flags - if bTransport == false, then ALL are disabled
  // NOTE: final val ==> the compiler will not even generate code into the .class file for any
  //       statement of the form if(flag){ .... }.
  //
  //       Change 'final val' to 'var' if you want to change these dynamically at runtime.
  final val bTransport         = true
  // Debug when one of the TransportApplication methods is called
  final val bTransportApp      = bTransport && true
  final val bAppReceived       = bTransportApp && true
  final val bAppSent           = bTransportApp && true
  final val bAppConnOpen       = bTransportApp && true
  final val bAppConnClose      = bTransportApp && true

  final val bTransportSetup    = bTransport && true
  final val bTransportOpen     = bTransport && true
  final val bTransportClose    = bTransport && false
  final val bTransportActor    = bTransport && true
  final val bTransportOutbound = bTransport && true
  final val bTransportOutDtl   = bTransport && true
  final val bTransportInbound  = bTransport && true
  final val bTransportInDtl    = bTransport && true
  final val bTransportDeChunk  = bTransport && true
  final val bTransportDoChunk  = bTransport && true
  final val bTransportSegment  = bTransport && true
  final val bTransportRdCycle  = bTransport && true
  final val bTransportBfrExpand= bTransport && false
  final val bTransportRedirect = bTransport && true
  final val bTransportOTW      = bTransport && true

  final val bFileTransfer      = bTransport && true
  final val bFTIn              = bFileTransfer
  final val bFTInReceive       = bFileTransfer && true
  final val bFTInMsg           = bFTIn && true

  final val bFTOut             = bFileTransfer
  final val bFTOutReceive      = bFTOut && true
  final val bFTOutMsg          = bFTOut && true


  final val nNullWriteCycles   = if(bTransport) 500 else 0
  final val nEmptyReadCycles   = if(bTransport) 20 else 0
  final val nTransportSleep    = if(bTransport) 0 else 0
}
