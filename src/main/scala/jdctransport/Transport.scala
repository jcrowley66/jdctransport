package jdctransport

import java.nio.{Buffer, ByteBuffer}
import java.nio.file._
import java.nio.channels._
import java.util.ArrayDeque
import scala.concurrent.duration.Duration
import java.util.concurrent._
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.atomic._
import akka.actor.{Props, ActorContext, Actor, ActorSystem, ActorRef}
import jdccommon.JDCFlagsCodes
import jdctransport.Transport._

import scala.collection.concurrent.TrieMap

/** Trait must be supported by the Application. Required methods must be supplied by the Application.
 *  Optional methods may be overridden if desired, otherwise default to no-ops */
trait TransportApplication {
  /** REQUIRED -- Message has been received (note, may be part of a file transfer, see transSuppressFileTransfer)
    *             The application should dispatch this to the correct handling method **/
  def transMsgReceived(msg:TransMessage, tran:Transport) = {}

  /** Optional -- application name for this App/Transport - used only in some log messages */
  def transAppName:String = "(NoName)"

  /** Optional -- New ConnID has been established */
  def transConnOpen(connID:Int, trans:Transport):Unit = {}

  /** Optional -- notification that this Transport has closed -- will return Exception if closed because of error
   * @param connID -- individual connID that was closed or 'connIDAll' it entire Transport has been closed
   **/
  def transClosed(connID:Int, trans:Transport, ex:Option[Exception] = None) = {}

  /** Optional -- determines whether Transport de-chunks inbound messages or returns chunks directly to the application
   *           -- large outbound messages will always be chunked
   *           -- per-message
   ****/
  def transSkipDeChunk():Boolean = false
  /** Optional -- Message passed to transMsgSend(..) has physically been sent (Note: passed to the outbound socket, it
    *             may not yet have been received by the target).  **/
  def transMsgSent(msg:OTWMessage, trans:Transport):Unit = {
    if(bTransportTrace) debugTrace(s"TransMsgSent-${this.getClass.getName}", msg, trans)
  }

  /** Optional -- Override if the application has defined additional TFlag values (see TMessage) */
  def transAllTFlags:List[TFlag] = TFError.allTFlags
  /********************************************************************************************************************/
  /** File Transfer operations - all are OPTIONAL if no File Transfers are initiated -- i.e. transferFile(..) called  */
  /**                                                                                                                 */
  /** Note: If any method returns an Option[FTAppData] that is non-empty, that instance will replace the current      */
  /**       instance within the FTInfo instance.                                                                      */
  /********************************************************************************************************************/
  /** Optional -- MUST override to allow File Transfers -- determine if this File Transfer is supported by the Application.
   *              NOTE: May be given an empty FTInfo just to get a general yes/no.
   **/
  def transAllowFT(info:FTInfo):(Boolean, Option[FTAppData]) = (false, None)
  /** Optional -  Return TRUE if application handled the FTStart, if FALSE then Transport handles it
   *              NOTE: Invoked only if transAllowFT returned a (true, _)
   **/
  def transFTStartHandled(info:FTInfo, msg:TransMessage):Boolean = false
  /** Optional -- terminate the transfer of data -- usually for an isGrowing=true file */
  def transStopTransfer(info:FTInfo):Unit = {}
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
  /** Optional -- Invoked during the stages of a file transfer - see values in object FTInfo. **/
  def transFTStages(trans:Transport, info:FTInfo):Unit = {}
  /** Optional -- called on each file transfer segment sent */
  def transFTSent(trans:Transport, info:FTInfo, msg:TransMessage):Unit = {}
  /** Optional -- called on each file transfer segment received **/
  def transFTReceived(trans:Transport, info:FTInfo, msg:TransMessage):Unit = {}
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
// Also passed to the DelayFor logic to delay & SendMessage traits
sealed trait TransportStep         // NOTE: This is used in several different classes
private final case class ReadBaseInfo() extends TransportStep   // A read - just get the base information
private final case class ReadBaseMore() extends TransportStep   // Read the rest of the base information
private final case class ReadRemaining()extends TransportStep
private final case class WriteInitial() extends TransportStep   // Initial Write of a Message
private final case class WriteMore()    extends TransportStep   // ... continue Writing the current Message

private final case class CheckQueue()    extends TransportStep  // Used by Application Actors with the SendMessage trait
private final case class FTXfrN()        extends TransportStep  // Used by File Transfer actors
private final case class FTClear()       extends TransportStep
/** Message sent to the TransportActor (and the In and Out Actors) to close down a particular
 *  Connection. If connID == -1, or this is the LAST active connID, then do a complete shutdown
 *  Message is also passed to the Application for that Connection (or ALL Applications)
 *  @param connID    - connID to close or
 *  @param clearOutQ - False == outbound actor should send all Queued messages, then close the connection
 *                     True  == drop any messages in the Queue for this connID
 *  @param processedAgain - True == we already processed any messages already in the Q, so time to close up shop
 *  @param andShutdown    - True == IFF there are no more active ConnIDs, then shut down the transport
 **/
final case class Close(connID:Int, clearOutQ:Boolean = false, andShutdown:Boolean = false, ex:Option[String] = None) extends TransportStep {
  def toShort = s"Close[ConnID: ${if(connID == Transport.connIDAll) "ALL Connections" else s"ConnID: $connID"}, ClearQ: $clearOutQ]"
}

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
 * NOTE: After a new Transport(....) and any other setup, you must call the start(....) method!
 *
 * @param nameIn    - User specified name for this transport system. Used for Actor names & user messages (debug, info, warn, error)
 *                    NOTE: Must contain only [a-zA-Z0-9] plus non-leading '-' or '_'
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
                val backPressureHigh  :Int = defaultBackpressureHigh, val backPressureLow:Int = defaultBackpressureLow,
                val FTBackPressureHigh: Int = defaultFTBackPressureHigh, val FTBackPressureLow:Int = defaultFTBackpressureLow
) {
  import Transport._

  override def toString = toShort

  def toShort           = s"Transport[$name $transID/$channelID --> ${channel.getRemoteAddress.toString}, Started: $started]"

  val transID     = assignTransID                 // Unique ID (within a single JVM) for this Transport, must be within 16 bits (unsigned)
  val channelID   = assignChannelID               // Unique ID for this Channel within this JVM

  val transChnlID = s"$transID/$channelID"

  @volatile
  private[jdctransport] var _inIsClosed = false
  @volatile
  private[jdctransport] var _outIsClosed = false

  def isClosed = _inIsClosed && _outIsClosed

  Transport.allTransports += (transID -> this)    // Add to global list of active Transports

  private var userAssignedID = 0
  /** User can assign a user-specific ID, but only once (to make sure it stays unique) */
  def getUserID           = userAssignedID
  def setUserID(id:Int)   = synchronized {
    if(userAssignedID == 0 ) userAssignedID = id
    else                     throw new IllegalStateException(s"assignUserID can only be called once!")
  }

  def name        = f"Transport: $nameIn - $transID/$channelID"
  val connections = new TrieMap[Int, (Boolean, TransportApplication)]()   // connID -> (true if closed, application)
                                                                            // NOTE: will have 0 -> app created by StartClient

  /** Define a ConnID, return None if is not already defined, previous App if this is a redefine */
  def defineConnID(connID:Int, app:TransportApplication):Option[(Boolean, TransportApplication)] = {
    connections.get(connID) match {
      case None                   => connections += (connID -> (false, app))
                                     None
      case Some((closed, oldApp)) => info(s"Trans $name, ConnID: $connID switching from ${oldApp.transAppName} to ${app.transAppName}")
                                     connections += (connID -> (false, app))
                                     Some((closed, oldApp) )
    }
  }

  val actorSystem = actorOpt.getOrElse(ActorSystem.create(s"Trans_$channelID"))

  // Redirect some TMessages to an ActorRef other than the normal transMsgReceived call
  // Expected to be low-volume, mainly to redirect File Transfers
  //                                 msgKey -> ActorRef
  val redirects   = new TrieMap[Long, ActorRef]

  /** Redirect a message if it is in the re-direct table */
  def wasReDirected(msg:OTWMessage):Boolean = {
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
    redirects += ( redirectKey(connID, msgID) -> to)
    if(bTransportRedirect) debug(s"ReDirect set ConnID: $connID, MsgID: $msgID, ActorRef: $to")
  }
  // Drop a REDIRECT given a FTInfo
  def dropRedirect(info:FTInfo):Unit = dropRedirect(info.request.connID, info.xfrMsgID)
  // Drop a REDIRECT
  def dropRedirect(connID:Int, msgID:Long):Unit = {
    if(bTransportRedirect){
      val ref = redirects.get(redirectKey(connID, msgID))
      debug(s"Remove Redirect -- ConnID: $connID, MsgID: $msgID, ActorRef: $ref")
    }
    redirects -= redirectKey(connID, msgID)
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
  private[jdctransport] val outbound= actorSystem.actorOf(Props(new TransportOutActor(this)))
  private[jdctransport] val inbound = actorSystem.actorOf(Props(new TransportInActor(this)))
  /////////////////////////////////

  val outboundCnt = new AtomicInteger     // Total outbound messages (so apps can throttle if needed)
                                          // Application (or SendMessage trait) bumps, outbound actor decrements
  val outboundData= new AtomicLong()      // Total data (length of all TMessages)
  val inFlight    = new AtomicInteger()   // Count of # passed to TransportOutActor - used for backpressure

  def sendMessageWillBlock = outboundCnt.get > maxOutCnt || outboundData.get > maxOutData

  /** Send a Message -- will block if max in-flight count or data is already exceeded */
  def sendMessage(msg:TransMessage):Boolean = {
    traceProcess(this, TraceSend, msg)
    if(msg.isValid){
      if(bTransportTrace) debugTrace(s"TransSendOTW", msg, this)
      while(sendMessageWillBlock) threadSleep(50)
      if(bTransportSendMsg){
        debug(s"SendMessage $transID/$channelID OTW: ${msg.toShort}")
      }
      outboundCnt.incrementAndGet             // Adjusted if msg must be auto-chunked
      outboundData.addAndGet(msg.length)      // ditto
      ttlMessages.incrementAndGet             // The global Transport message count
      ttlData.incrementAndGet                 // The global Transport data in process amount
      outbound ! msg
      true
    } else {
      error(s"Transport.sendMessage -- Message not valid -- ${msg.toShort}")
      false
    }
  }

  // Map of onFinish methods passed to the 'transferFile' method
  private[jdctransport] val onFinishMap = new TrieMap[Long, (FTInfo) => Unit]

  private val onFinishNoOp:(FTInfo) => Unit = (noop:FTInfo) => {}

  /** Execute a File Transfer - Note: the transFTInboundPath and transFTInboundDone MUST be provided by the Application
   *                                  on the inbound side of this transfer.
   *  @param infoIn   - information about the transfer - msgID, connID<
   *  @param onFinish - optional method which will be invoked when transfer is completed - either successfully or in error (check code)
   *                    (The transFTStage method may also be overridden to detect the close of the transfer on either end, that
   *                     will be called before onFinish)
   *                    Note: Invoked on this end of the transfer regardless of whether a transfer in or out.
   *
   *  @return empty if OK, else an error message
   **/
  def transferFile(infoIn:FTInfo, onFinish:(FTInfo) => Unit = onFinishNoOp):String = {
    val (bool, appDataOpt) = app.transAllowFT(infoIn)
    val info = updateInfo(infoIn, appDataOpt)
    if(bool){
      if(! (onFinish eq onFinishNoOp)) onFinishMap += (info.infoKey -> onFinish)
      if(info.isInbound) actorSystem.actorOf(Props( new FileTransferIn(this, info)))
      else               actorSystem.actorOf(Props( new FileTransferOut(this, info)))
      ""
    } else {
      val errMsg = "transAllowFT returned a False"
      val err    = Error(connID = info.request.connID, msgID = info.xfrMsgID, errCode = ErrorCodes.notAllowed, errors = List(errMsg))
      onFinish(info.copy(error = err))
      errMsg
    }
  }
  @volatile
  private var started = false
  /** In Publisher/Xchange usually called when a Socket.accept() is triggered.
   *  ConnID is assigned, then this method called to respond to the connectee
   *  with the TFConnCreated message
   *  @param name - name desired by the caller to be assigned to the TMessage
   *  @param data - data payload desired by the caller to be assigned to the TMessage
   **/
  def start(connID:Int, msgID:Long, app:TransportApplication, name:String = "", data:Array[Byte] = Array[Byte]()) =
    if(!started){
      started = true
      val conn = StartConn(connID, app)
      startConn(name, conn, this, sendToApp = true)
      toInAndOut(conn)
      if(connID != 0) {
        connections += (connID -> (false, app))
        // Send msg back to the connecting task with the connID assigned
        val msg = TMessage(trans = this, connID = connID, msgID = msgID, flags = TFConnCreated.flag, name = name, data = data)
        sendMessage(msg)
      }
    } else
      warn(s"Ignoring start(...) call, already started: $toShort")

  def wasStarted = started

  /** Completely close down this Transport - all connIDs. ASSUMES complete shutdown */
  def closeAll(clearOutQ:Boolean = true, ex:Option[Exception] = None) =
        close(connIDAll, clearOutQ, andShutdown = true, ex)

  /** Close down the given connID (or connIDAll) -- either process any pending outbound messages or clear them*/
  def close(connID:Int, clearOutQ:Boolean = true, andShutdown:Boolean = false, ex:Option[Exception] = None) = {
    if (bTransportClose) debug(s"$name -- Closing ConnID: $connID")
    if(ex.nonEmpty)
      error(s"Closing Transport $transID because of Exception: ${ex.get.toString}")
    val cls = Close(connID, clearOutQ, andShutdown, if(ex.isEmpty) None else Some(ex.get.toString) )
    if (connID == connIDAll) {
      outbound ! cls
      inbound  ! cls
    } else connections.get(connID) match {
      case None      => warn(f"Close for ConnID: ${connID}%,d, but not found -- ignored")
      case Some((closed, app)) => if(closed==false) {
                                    connections += (connID -> (true, app))
                                    outbound ! cls
                                  }
    }
  }
  // Close down this Transport - will issue Close messages for ALL connIDs
  def shutdownTrans(wait:Boolean = true): Unit = {
    closeAll(clearOutQ=false, ex = None)
    while( wait && !isClosed ) try{ Thread.sleep(100) } catch { case _:Exception => }
    try {
      connections.clear
      actorSystem.stop(inbound)
      actorSystem.stop(outbound)
      if(channel.isOpen) channel.close
    } catch {
      case _:Exception => // no-op -- ignore any errors
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
    lastDelay = if(lastDelay<=0) actorSleepInitial else if( lastDelay >= actorSleepMax) actorSleepMax else lastDelay * 2
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
  var info:FTInfo         // So we can update the # of segments and data sent

  // Logic - when the inFlight count is > maxInFlight, then enter backpressure mode and just queue all messages
  def minInFlight:Int     // 0 == no min, start sending again as soon as below maxInFlight
                          // n == start sending when actual in-flight < n
                          //      Note: n == 1 means start sending when 0 in-flight
  def maxInFlight:Int     // 0 == NO backpressure

  private var isInBackpressure = false

  // Queue of Message's to be sent. Messages added here if handling back-pressure
  private val queue    = new ArrayDeque[TransMessage]()

  private def addToQueue(msg:TransMessage)  = queue.add(msg)

  // If ftInfo.nonEmpty, then this came from a File Transfer
  protected def internalSendMessage(msg:TransMessage, ftInfo:Option[FTInfo]) = {
    addToQueue(msg)
    internalSendQueued(ftInfo)
  }

  // If ftInfo.nonEmpty, then this came from a File Transfer
  protected def internalSendQueued(ftInfo:Option[FTInfo]):Unit = {
    // Send one message from the Q, and call again to check the next one
    def sendOne = if (!queue.isEmpty) {
                          val msg = queue.poll
                          trans.outbound ! msg

                          if(msg.isFTSegment)
                              info = info.copy(chunksSent = info.chunksSent + 1, totalData = info.totalData + msg.data.length)
                          trans.app.transFTSent(trans, ftInfo.get, msg)

                          delayReset
                          internalSendQueued(ftInfo)
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
object Transport extends JsonSupport {
  // INJECT methods for debug, info, warn, error messages - default is just print to the console
  var debug:(String) => Unit = (str:String)  => println(s"DEBUG: $str")
  var info:(String)  => Unit  = (str:String) => println(s" INFO: $str")
  var warn:(String)  => Unit  = (str:String) => println(s" WARN: $str")
  var error:(String) => Unit = (str:String)  => println(s"ERROR: $str")

  // Assign a unique TransID and ChannelID within this JVM. Uses the same AtomicInteger, so will never be ==
  // WARNING: Depends on a VAL transID and VAL channelID in Transport so only called once per Transport
  private val transChnlID = new AtomicInteger
  def assignTransID       = transChnlID.incrementAndGet
  def assignChannelID     = transChnlID.incrementAndGet

  val allTransports = new TrieMap[Int, Transport]         // ALL active Transport instances
  val ttlMessages   = new AtomicLong()                    // Total TMessages in-process in all active Transports
  val ttlData       = new AtomicLong()                    // Total data in-process in all active Transports

  /** Create a Long key from a 'short' ID (e.g. connID) & msgID */
  def makeKey(shortID:Int, msgID:Long):Long = if(shortID < maskShort)
                                                ((shortID.toLong << shiftShort) | msgID)
                                              else
                                                throw new IllegalStateException(s"shortID must be 16-bit unsigned, had ${shortID}")

  /** Parse a msgKey into the connID, msgID pieces --> (connID, msgID) */
  def parseKey(key: Long): (Int, Long) = ((key >> shiftShort).toInt, (key & maskMsgID))

  /** Parse a msgKey and return as a string of the form -- connID/msgId */
  def parseKeyStr(key:Long):String = { val (connID, msgID) = parseKey(key); f"$connID/$msgID%,d" }

  /** Create a key for the Redirect table */
  def redirectKey(connID:Int, msgID:Long):Long = makeKey(connID, msgID)

  /** If one of the app calls returned a new FTAppData, update the FTInfo instance */
  def updateInfo(info:FTInfo, appDataOpt:Option[FTAppData]):FTInfo = if(appDataOpt.isEmpty) info else info.copy(appData = appDataOpt.get)



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

  val connIDBroadcast= -2                 // Broadcast to all connIDs
  val connIDAll      = -1                 // Some messages (e.g. Close) apply to ALL connIDs

  val connectionID   = new AtomicInteger    // Assignment of ConnectionIDs -- Server-side use only

  val actorSleepInitial = 8         // If waiting to read/write channel, first sleep interval ... then double it - millis
  val actorSleepMax     = 256       // ... until exceeds this max wait time
                                    // Note: SleepInitial & SleepMax should both be powers of 2 so lastDelay * 2 will == SleepMax

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

  val maskSzName    = 0x000003FF        // Mask to pick up the szName (after shift)
  val maskFlags     = 0x003FFFFF        // Mask to pick up the Flags
  val maskByte      = 0x00FF
  val keyHighBits   = 16
  val shiftShort    = 64 - keyHighBits
  val maskShort     = 0x0000FFFF
  val maskMsgID     = 0x0000FFFFFFFFFFFFL
  val maskInt       = 0x00FFFFFFFFL

  val maxSzName     = Int.MaxValue & maskSzName
  val maxFlagValue  = Int.MaxValue & maskFlags

  val bitsInSzName  = java.lang.Integer.bitCount(maskSzName)
  val bitsInFlags   = java.lang.Integer.bitCount(maskFlags)

  val szNameShift   = 32 - bitsInSzName // Shift a LONG right to get szName to low-order bits

  val bitCountOK    = if(bitsInSzName + bitsInFlags != 32 ||
                        ( (maskSzName.toLong << szNameShift) & maskFlags.toLong) > 0 ||   // Overlap
                        ( (maskSzName<<szNameShift) >>> szNameShift != maskSzName) ){        // lose bits
                        throw new IllegalStateException("maskSzName and/or maskFlags is screwed up ")
                        false
                      } else
                        true

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

  /** One of the actors got a StartConn message - update the given Map in place
   *  and (optionally) send an AppConnect message to the Application
   **/
  def startConn(label:String, conn:StartConn, trans:Transport, sendToApp:Boolean = false) = {
    if (bTransportOpen) debug(s"$label STARTING ${conn.toShort}")
    trans.connections.get(conn.connID) match {
      case None      => trans.connections += (conn.connID -> (false, conn.app))
      case Some((closed, ref)) => if (ref != conn.app) {    // If ==, have a dup message so just ignore
                                    warn(f"$label -- ConnID: ${conn.connID}%,d was already defined -- changing Application reference")
                                    trans.connections += (conn.connID -> (false, conn.app) )
                                  }
    }
    if(sendToApp) conn.app.transConnOpen(conn.connID, trans)
  }

  lazy val transBrkDummy = new AtomicInteger()

  def transBrk(name:String) = {
    if(bTransportBrkPt) {
      if(lstTransBreakpoint.contains("*") || lstTransBreakpoint.contains(name)){
        // Set the breakpoint here!!!!
        transBrkDummy.incrementAndGet     // Only so JIT compiler doesn't decide to eliminate transBrk completely
      }
    }
  }
  /************************************************************************************************/
  /** Injection of message trace/modify functions IFF caller desires                              */
  /**                                                                                             */
  /** The intent is to allow an application to establish tracing or modification of messages at   */
  /** key points external to this Transport system, with minimal modification to the application  */
  /** code itself.                                                                                */
  /**                                                                                             */
  /** WARNING: Each TraceMethod is normally expected to issue debug traces of message activity,   */
  /**          BUT the returned TransMessage will then be passed along the list and eventually    */
  /**          for transport (if sendMessage was called) or to the application (before calling    */
  /**          TransportApplication.transMsgReceived) so a TraceMethod may in face MODIFY the     */
  /**          message. Use carefully!                                                            */
  /************************************************************************************************/

  /** A TraceMethod will be invoked at the appropriat point. It is passed:
   *   -- Int         - the unique ID assigned internally to this TraceMethod when it is add'ed
   *   -- TracePoint  - the point at which it is being invoked
   *   -- Transport   - the Transport involved
   *   -- TransMessage- the message involved.
   *
   * RETURN: Usually the original TransMessage, but it may (carefully!) be modified if desired
   **/
  type TraceMethod = (Int, TracePoint, Transport, TransMessage) => TransMessage

  private var traceNextID = new AtomicInteger(0)
  // All registered TraceMethod functions
  private var traceList:List[(Int, TracePoint, TraceMethod)] = Nil

  private[jdctransport] def traceProcess(trans:Transport, forPoint:TracePoint, msg:TransMessage):TransMessage = {
    if(traceList.isEmpty)
      msg
    else {
      var result = msg
      traceList.view
               .filter { case(_, point, _) => point.is(forPoint) }
               .foreach{ case(id, _, func) => result = func(id, forPoint, trans, result)}
      result
    }
  }
  /** Add a TraceMethod to the list of trace methods to be invoked
   *
   * @param forPoint  - when this traceFunc should be invoked: for sends, receives, always, etc
   * @param traceFunc - the function to be executed
   * @return          - the internal ID assigned to this entry (needed for traceDrop)
   */
  def traceAdd(forPoint:TracePoint, traceFunc:TraceMethod):Int = synchronized{ val id = traceNextID.incrementAndGet; traceList +:= (id, forPoint, traceFunc); id }
  /** Drop the TraceMethod with the given ID from the active trace list */
  def traceDrop(id:Int) = synchronized{ traceList = traceList.filterNot( _._1 == id )}

  /** ByteBuffer position(n) + other routines will abort when running on Java version < 9 if compiled by 9 or higher
   *  Calling these methods skirt that problem.
   *  see:     https://www.morling.dev/blog/bytebuffer-and-the-dreaded-nosuchmethoderror/
   *           https://github.com/apache/felix/pull/114
   */
  def bbPosition(bb: ByteBuffer, n: Int): ByteBuffer = bb.asInstanceOf[Buffer].position(n).asInstanceOf[ByteBuffer]

  def bbLimit(bb: ByteBuffer, n: Int): ByteBuffer = bb.asInstanceOf[Buffer].limit(n).asInstanceOf[ByteBuffer]
  
  /************************************************************************************************/
  /** Internal debug flags - require a rebuild of jdctransport to take effect                     */
  /**                                                                                             */
  /** Notes: -- the 'final val' allows the compiler to not even generate code for any section of  */
  /**           the form:  if(flag){.....} if 'flag' is false                                     */
  /**        -- generally structured in logical sections:                                         */
  /**             bTransport -- the MASTER, if 'false' then all other flags are also 'false'      */
  /**             See: bTransportApp -- serves as a sub-master to control a section of flags      */
  /**                                                                                             */
  /************************************************************************************************/
  final val bTransport         = false
  final val bInboundFunc       = bTransport && false  // run inboundTest on each msg & if true call inboundFunc
                                                      // Can just print it, set a breakpoint, change the func, etc
  var inboundTest = (msg:TransMessage) => { false }   // change to pick out messages of interest, default to 'false'
  var inboundFunc = (msg:TransMessage) => {           // and if TRUE, then execute this function
    println(s" INBOUND: ${msg.toShort}")
  }
  final val bOutboundFunc      = bTransport && false  // Ditto for outbound messages
  var outboundTest = (msg:TransMessage) => false
  var outboundFunc = (msg:TransMessage) => {
    println(s"OUTBOUND: ${msg.toShort}")

  }
  final val bTransportTrace    = bTransport && false
  final val lstTraceMsgs       = List.empty[String]

  final val bTransportBrkPt    = bTransport && false           // Allow setting a breakpoint on transBrk()
  final val lstTransBreakpoint = List.empty[String]        // ... but only for these message names (* == all messages)

  // Debug when one of the TransportApplication methods is called
  final val bTransportApp      = bTransport && false
  final val bAppReceived       = bTransportApp && false
  final val bAppSent           = bTransportApp && false
  final val bAppConnOpen       = bTransportApp && false
  final val bAppConnClose      = bTransportApp && false

  final val bTransportOpen     = bTransport && false           // Startup of Transport & all Actors
  final val bTransportClose    = bTransport && false
  final val bTransportActorID  = bTransport && false
  final val bTransportSendMsg  = bTransport && false
  final val bTransportOutbound = bTransport && false
  final val bTransportOutDtl   = bTransport && false
  final val bTransportInbound  = bTransport && false
  final val bTransportInDtl    = bTransport && false
  final val bTransportDeChunk  = bTransport && false
  final val bTransportDoChunk  = bTransport && false
  final val bTransportSegment  = bTransport && false
  final val bTransportRdCycle  = bTransport && false
  final val bTransportBfrExpand= bTransport && false
  final val bTransportRedirect = bTransport && false
  final val bTransportOTW      = bTransport && false

  final val bTransportDelayIn  = bTransport && false         // Insert artificial delay on INBOUND processing
  final val nTransportDelayIn  = 5000                       // IFF bTransportDelayIn, number of millis to delay

  final val bFileTransfer      = bTransport && false
  final val bFTIn              = bFileTransfer
  final val bFTInReceive       = bFileTransfer && false
  final val bFTInMsg           = bFTIn && false

  final val bFTOut             = bFileTransfer
  final val bFTOutReceive      = bFTOut && false
  final val bFTOutMsg          = bFTOut && false


  final val nNullWriteCycles   = if(bTransport) 500 else 0
  final val nEmptyReadCycles   = if(bTransport) 20 else 0
  final val nTransportSleep    = if(bTransport) 0 else 0

  // Some column widths to attempt to line up the information in the log lines
  val szTrcLabel = 60
  val szTrcTrans = 60
  val traceMarker= "*" * 4 + "--> "
  /**
   * Log a particular set of messages (by name) -- NOTE: Similar method also exists in Transport!
   * @param label - label to indicate where log originated (e.g. TransportOutActor in Transport layer)
   * @param msg   - the message being processed
   * @param trans - if non-null, ID info will be included in log msg
   * @param msgs  - list of messages to be traced
   */
  def debugTrace(label:String, msg:TransMessage, trans:Transport = null, msgsToTrace:List[String] = lstTraceMsgs) = {
    def padTo(str:String, n:Int) = if(str.length >= n) str else str + " " * (n - str.length)
    if(bTransportTrace) {
      if(msgsToTrace.nonEmpty) {
        val szTrcName = msgsToTrace.map(_.length).max
        val name = msg.name
        if (msgsToTrace.contains(name)) {
          val transStr = if (trans != null) s"${trans.transID}/${System.identityHashCode(trans.channel)}" else "(NoTrans)"
          debug(s"$traceMarker${padTo(name, szTrcName)} ${padTo(label, szTrcLabel)} ${padTo(transStr, szTrcTrans)} ${msg.toShort}")
        }
      }
    }
  }
}

/** Define at which point various registered TraceMethod's will be invoked */
sealed trait TracePoint extends JDCFlagsCodes {
  val bitFlags    = true
  val allActive   = List(TraceSend, TraceSent, TraceRcv, TraceSendRcv, TraceFT)
  val allExtended = allActive
  override val allExcluded = List(TraceNone, TraceAll)
}
case object TraceNone    extends TracePoint { val value = 0;                                lazy val name = "TraceNone";     lazy val description = "Trace NONE"}
case object TraceSend    extends TracePoint { val value = 0x01 << 1;                        lazy val name = "TraceSend";     lazy val description = "Invoke when Transport.sendMessage is called"}
case object TraceSent    extends TracePoint { val value = 0x01 << 2;                        lazy val name = "TraceSent";     lazy val description = "Invoke when the message is actually sent to the Socket"}
case object TraceRcv     extends TracePoint { val value = 0x01 << 3;                        lazy val name = "TraceRcv";      lazy val description = "Invoke just before calling TransportApplication.transMsgReceived"}
case object TraceSendRcv extends TracePoint { val value = TraceSend.value + TraceRcv.value; lazy val name = "TraceSendRcv";  lazy val description = "Invoke on either a Send or Rcv"}
case object TraceFT      extends TracePoint { val value = 0x01 << 4;                        lazy val name = "TraceFT";       lazy val description = "Invoke on File Transfers" }
case object TraceAll     extends TracePoint { val value = 0xFF;                             lazy val name = "TraceAll";      lazy val description = "Invoke TraceMethod for all TracePoints"}

trait TransActor extends Actor {
  def trans:Transport
  val ident = { if(Transport.bTransportActorID) Transport.debug(s"ACTOR ${this.getClass.getName} has ActorRef: $self"); 0}
}
