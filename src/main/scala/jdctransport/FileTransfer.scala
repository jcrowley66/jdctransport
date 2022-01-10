package jdctransport

import java.nio._
import java.nio.file._
import java.nio.channels._
import java.util.Arrays
import akka.actor._
import spray.json._

import java.nio.file.attribute.{BasicFileAttributes, FileTime, BasicFileAttributeView}

import FileTransfer._
// FIXME -- did not test the 'isGrowing' logic AT ALL, open question whether channel.read will return 0 if there
//          is no more data but some other process is writing to the file, or -1???

/**
 * Transfer a file between two machines or copy a file to another location on the same machine.
 * -- FileTransferOut - an Actor that does the SEND side of the transfer
 * -- FileTransferIn  - an Actor that does the RECEIVE side of the transfer
 *
 * Both of these Actors are a one-use only, do the transfer, and then kill themselves.
 *
 * NOTE: Transport.transferFile is the method invoked by either end of a Transport. One side of the Transport will
 *       start a FileTransferOut actor, the other the FileTransferIn, and will coordinate internally so that both are
 *       ready - then start the actual physical transfer.
 **/

/** Application specific information if desired.
 *
 * This appears in 2 places:
 *  1) in FTRequest it is immutable, and
 *  2) in FTInfo an updated instance may be returned by some of the application callback methods, and this version
 *     will replace the current version in FTInfo
 *
 * The FileTransfer logic does not use the information here at all, it is purely for use by the calling application.
 **/
final case class FTAppData (
  appIDa:Long       = 0,
  appIDb:Long       = 0,
  appIDc:Long       = 0,
  appStringA:String = "",   // Since entirely handled by the application, can be, e.g., a JSON structure with multiple fields, etc.
  appStringB:String = "",
  appStringC:String = "",
  appUser:String    = ""    // Application specific "user" information, also may be a simple String, JSON, etc
)
/** Request passed to Transport.transferFile method to initiate a file transfer. All of these variables are immutable
 *  except the isInbound flag will be set as needed when passed to the other transport.
 **/
final case class FTRequest(
    connID:Int              = -1,           // connID for this transfer - must be 16-bit unsigned value
    appFileName:String      = "",           // May be absolute, relative, or anything else. The TransferApplication
                                            // transFTOutboundPath and transFTInboundPath must be able to interpret
                                            // and return an absolute Path
    isGrowing:Boolean       = false,        // Source file is actively growing (e.g. some log file) so keep transferring
                                            // any new data to the destination until the Application explicitly stops it
    reportEvery:Int         = 0,            // 0==never, else report every N data chunks sent/received
    markReadOnly:Boolean    = false,        // Mark the INBOUND file as read-only once it is completely transferred
    preserveTimes :Boolean  = true,         // Preserve the times - created, modified, accessed - from the source file on the target file
    append:Option[Boolean]  = None,         // Some(true) = append to any existing file on inbound end, Some(false) = truncate file on inbound side,
                                            // None = terminate with error if inbound file already exists
    appData:FTAppData       = FTAppData()
) {
  override def toString = f"FTRequest[connID: $connID%,d, FileName: $appFileName, Growing: $isGrowing, RptEvery: $reportEvery, MarkRO: $markReadOnly, PrsvTimes: $preserveTimes, Append: $append]"
}
final case class FTInfo(  stage:Int           = StageNone.code, // See FTStage definitions below
                          xfrMsgID:Long       = 0,              // ALL TMessages will use this same ID for this transfer.
                          isOriginator:Boolean= false,          // Whether we are the originating end of the transfer
                          isInbound:Boolean   = false,          // Modified as needed when reported to inbound or outbound application
                          request:FTRequest   = FTRequest(),    // the original request
                          error:Error         = Error(),        // 'hasError' method below determines if an error has occured
                          totalSize:Long      = 0,              // Initialized by outbound size (if isGrowing, this is initial size)
                          chunksTtl:Int       = 0,              // Total number of chunks to be sent (if isGrowing, based on initial size)
                          chunksSent:Int      = 0,              // Number of chunks sent - updated on outbound & inbound side
                          totalData:Long      = 0,              // Updated on outbound side, computed on input side
                          // These are set by outbound side if preserveTimes, used by inbound side
                          millisCreated:Long  =0, millisChanged:Long=0, millisLastRef:Long=0,
                          // Set by outbound side & reported to application on outbound side
                          millisStart   :Long = 0, millisFirstData:Long = 0, millisEnd:Long = 0,
                          appData:FTAppData   = FTAppData()
                          ){

  def hasError = error.nonEmpty
  def getStage = StageStart.find(stage)

  override def toString = strShort

  def strShort = f"""FTInfo[Request: $request, Inbound: $isInbound, Stage: $stage, MsgID: $xfrMsgID%,d, Size: $totalSize%,d ${if(hasError) error.toString else ""}]"""
}

/****** Define the STAGEs of processing that may be reported to the Application *******/
sealed trait FTStage {
  def code:Int
  def label:String
  def description:String

  def allStages = List(StageNone, StageStart, StageReady, StageData, StageDone, StageStop, StageRefused, StageError)

  def find(stage:Int):FTStage = allStages.find( _.code == stage )match {
                                  case Some(stg) => stg
                                  case None => throw new IllegalArgumentException(s"Stage number $stage is not valid, must be one of ${allStages.map( _.code ).mkString(", ")}")
                                }
}
case object StageNone     extends FTStage { val code = -1; val label = "None";    val description = "NO Stage, should never occur!"}
case object StageStart    extends FTStage { val code = 0;  val label = "Start";   val description = "File transfer initiated by application"}
case object StageReady    extends FTStage { val code = 1;  val label = "Ready";   val description = "Both inbound & outbound sides are ready to start transfer"}
case object StageData     extends FTStage { val code = 2;  val label = "Data";    val description = "Data transfer in progress (may be reported multiple times)"}
case object StageDone     extends FTStage { val code = 3;  val label = "Done";    val description = "File transfer completed successfully"}
case object StageStop     extends FTStage { val code = 4;  val label = "Stop";    val description = "Other end issued a Stop order"}
case object StageRefused  extends FTStage { val code = 5;  val label = "Refused"; val description = "Other end refused File Transfer request"}
case object StageError    extends FTStage { val code = 99; val label = "Error";   val description = "Error has occured, transfer aborted"}

private final case class FTStart()
private final case class FTBackPressure()

/****************************************************************************************************/
/** File Transfer OUTBOUND                                                                          */
/****************************************************************************************************/
/** Generic implementation of an OUTBOUND File Transfer through the jdctransport mechanism.
 *
 *  This is in the form of an Actor which:
 *  -- will have a corresponding FileTransferIn on the other end of the Transport
 *  -- is a one-time-use Actor. It handles a single transfer, then shuts down.
 *  -- verifies existence and readability of the source file
 *  -- if this is the first one created, sends a message to the other end to create the FileTransferIn
 *     if other end already exists, just starts sending data
 *  -- sends a FileTransferStop if necessary because of errors at startup
 *  -- uses the SendMessage trait to handle throttling
 *  -- sends a FileTransfer final report to the application
 *  -- sends a FileTransferStop message to the other end
 *  -- terminates
 **/
class FileTransferOut(val trans:Transport, var info:FTInfo) extends Actor with JsonSupport with SendMessageTrait {
  import Transport._
  import FTInfo._

  // NOTE: All val's and initialization code executed when this instance is created. If there are errors, e.g. the
  //       file to be transferred does not exist, then will shut down upon getting FTReady message.
  //       Hence, several 'lazy val's used so these are not initialized unless needed.

  val name    = s"FTOUT ${info.request.appFileName} --"    // NOTE: Name of the Actor, not the Message

  //////////// The variables injected to SendMessage & DelayFor traits //////////////
  val actorContext  = context
  val actorSelf     = self
  val app           = trans.app

  override
  val minInFlight   = defaultFTBackpressureLow
  override
  val maxInFlight   = defaultFTBackPressureHigh         // NOTE: within THIS logic, also stops reading if > this
                                                        //       number of Messages in the SendMessage Q so that we
                                                        //       do not throw OOM
  /////////// End of variables injected into SendMessage & DelayFor traits /////////////

  val path                 = {  val (pathOpt, appOpt) = app.transFTOutboundPath(trans, info)
                                if (appOpt.nonEmpty) info = info.copy(appData = appOpt.get)
                                pathOpt.getOrElse(null)
                             }

  var fChannel:FileChannel = null

  val bfr        = new Array[Byte](maxDataIfNoName) // Input buffer for reads, then copy made for each Message sent
  val bb         = ByteBuffer.wrap(bfr)
  val buffer     = bb.asInstanceOf[Buffer]          // See https://stackoverflow.com/questions/61267495/exception-in-thread-main-java-lang-nosuchmethoderror-java-nio-bytebuffer-flip

  val hadError = if(path==null)
                   "No outbound file path returned by transFTOutboundPath"
                  else if(!Files.exists(path)) {
                    "File does not exist"
                  } else if(!Files.isReadable(path)) {
                   "File cannot be read"
                  } else if(!Files.isRegularFile(path)) {
                    "This is not a File, cannot be transferred"
                  } else try {
                    fChannel = FileChannel.open(path)
                    ""
                  } catch {
                    case ex:Exception => s"Path: ${path.toString} could not be opened, EX: $ex"
                  }

  // All these are 'lazy' since only used if did not have an error
  lazy val attrs      = Files.readAttributes(path, classOf[BasicFileAttributes])
  lazy val szChunk    = maxDataIfNoName            // Data sent in each chunk (chunks have no name, msgID identifies as part of transfer)
  lazy val chunksTtl  = (attrs.size / szChunk) + (if( (attrs.size % szChunk) != 0 ) 1 else 0) // Based on initial file size if isDynamic
  var numSent         = 0

  // If we are the originating end, must send a message to the other end to initialize & respond when ready
  lazy val (idMsg, isOriginatingEnd) = if(info.isOriginator){
                                        info = info.copy(isOriginator = false, totalSize = attrs.size )
                                        (info.xfrMsgID, true)
                                     } else
                                        (info.xfrMsgID, false)

  if(bFTOut){
    if(hadError.isEmpty) debug(s"$name -- ${info.request.toString}") else debug(s"$name Error: $hadError, Info: ${info.toString}")
  }

  if(hadError.isEmpty)
    self ! FTStart()
  else {
    reportError(true, trans, info, List(hadError), notifyOtherEnd = !isOriginatingEnd)
    context.stop(self)
  }

  def receive = {
    case _:FTStart        =>if(bFTOutReceive) debug(s"$name got FTStart")
                            // Size might have changed in an isGrowing situation
                            info = info.copy(stage = StageStart.code, totalData = attrs.size)
                            if(info.request.preserveTimes)
                              info = info.copy(millisCreated = attrs.creationTime.toMillis, millisChanged = attrs.lastModifiedTime.toMillis, millisLastRef = attrs.lastAccessTime.toMillis)

                            trans.setRedirect(info, self)

                            trans.app.transFTStages(trans, info)

                            if (isOriginatingEnd) {         // have to tell other end to start up the inbound actor, wait for a 'ready' msg
                              val msg = setupMessage(trans, info.copy(isInbound = true), name = Transport.transFTStart, self, false, true)
                              trans.sendMessage(msg)
                            } else {                        // Other end already going, just report status & start sending data
                              info = reportStage(StageReady, trans, info)
                              self ! FTXfrN()
                            }

    case msg:TMessage     => msg.name match {     // Note: transFTStart is not handled here but by the main Transport
                                                  //       since the msgID has not been redirected yet if the other
                                                  //       end initiated the whole process.
                                case Transport.transFTReady   =>info = reportStage(StageReady, trans, info)
                                                                self ! FTXfrN()       // Other end is ready, start sending

                                case Transport.transFTStop    => // Order to stop from the other end
                                                                stopThisEnd(true, StageStop, trans, info, Some(msg))
                                                                context.stop(self)

                                case Transport.transFTRefused =>// Other end refused the request for a file transfer
                                                                info = stopThisEnd(true, StageRefused, trans, info, Some(msg))
                                                                context.stop(self)

                                case unk                      => error(s"$name -- Msg Name $unk ignored by $info")
                             }

    case ack:ACK          => if(bFTOutReceive) debug(s"$name ACK: $ack")
                             if(ack.msg.msgID==info.xfrMsgID) {
                               sendQueued
                             }

    case nak:NAK          =>error(s"$name NAK ${nak.failReason}")
                            reportError(true, trans, info, List(nak.failReason.toString), notifyOtherEnd=true)
                            context.stop(self)

    case _:CheckQueue     => if(bFTOutReceive) debug(s"$name CheckQueue" )
                             sendQueued               // Handle back-pressure

    case _:FTClear        => if(bFTOutReceive) debug(s"$name FTClear")
                             // Clear any queued messages, then stop
                             if(sendQSize==0) {
                                info = stopThisEnd(true, StageDone, trans, info)
                                context.stop(self)
                              } else {
                                sendQueued
                                delayFor(FTClear())   // We're at end, so just spin until Q is empty
                              }

    case _:FTXfrN         => if(bFTOutReceive) debug(s"$name FTXfrN")
                             if(sendQSize > maxInFlight)       // Local back-pressure so we don't blow memory
                                delayFor(FTXfrN())
                              else {
                                buffer.position(0)
                                buffer.limit( szChunk )
                                delayReset
                                self ! ReadRemaining()
                              }

    case _:ReadRemaining  =>if(bFTOutReceive) debug(s"$name ReadRemaining")
                            fChannel.read(bb) match {
                              case 0  => delayFor(ReadRemaining())
                              case -1 => val msg = setupMessage(trans, info, "", self, true, false)
                                         if(!info.request.isGrowing){
                                            val flags= msg.flags | (if(numSent==0) FFirstChunk.flag else 0) | (if(info.request.isGrowing) 0 else FLastChunk.flag)
                                            val data = if(buffer.position() > 0) Arrays.copyOf(bfr, buffer.position()) else new Array[Byte](0)
                                            sendMessage(msg.copy(flags = flags , data = data))
                                            self ! FTClear()
                                         } else
                                           delayFor(ReadRemaining())  // Doing isGrowing, so take this as a false EOF until more data added to source file

                              case _ => val isFull = buffer.position() == buffer.limit()
                                        if(isFull || info.request.isGrowing) {        // For dynamic files, always send whatever we've got
                                          var msg = setupMessage(trans, info, "", self, true, false)
                                          if(numSent==0) msg = msg.copy(flags = msg.flags | FFirstChunk.flag)
                                          if(!info.request.isGrowing && numSent + 1 == chunksTtl) msg = msg.copy(flags = msg.flags | FLastChunk.flag)
                                          if(bFTOutMsg) debug(s"$name MSG: ${msg.strShort}")
                                          sendMessage(msg.copy(data = Arrays.copyOf(bfr, buffer.position)))
                                          numSent += 1
                                          if(info.request.isGrowing || numSent < chunksTtl)
                                            self ! FTXfrN()
                                          else
                                            self ! FTClear()
                                        } else
                                          self ! ReadRemaining()
                            }

    case unk              => Transport.warn(s"$name Did not recognize: $unk")
  }
}
/****************************************************************************************************/
/** File Transfer INBOUND                                                                           */
/****************************************************************************************************/
/**
 * Inbound side of a File Transfer. Accepts Message's from the basic Transport mechanism and writes the
 * file. NOTE: This will verify the expected msgID values and ignore all other Messages.
 *
 * This Actor terminates when processing is complete.
 */
class FileTransferIn(trans:Transport, var info:FTInfo) extends Actor with JsonSupport {
  import Transport._

  val name      = s" FTIN ${info.request.appFileName} --"    // NOTE: Name of the Actor, not the Message
  val path      = { val (pathOpt, appOpt) = trans.app.transFTInboundPath(trans, info)
                    if (appOpt.nonEmpty) info = info.copy(appData = appOpt.get)
                    pathOpt.getOrElse(null)
                  }
  val timeStart = System.currentTimeMillis

  var fileChannel:FileChannel = null

  val hadError= if(path==null)
                  "transFTInboundPath returned a None"
                else if(Files.exists(path)) {
                  try {
                  info.request.append match {
                    case Some(true)   => fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
                                          ""
                    case Some(false)  => fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
                                          ""
                    case None         => s"File $path already exists and the 'append' setting does not allow overwrite"
                  }
                } catch {
                    case ex:Exception => s"Could not open $path, EX: $ex"
                  }
                } else try {
                    fileChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
                    ""
                } catch {
                  case ex:Exception => s"$name Exception creating $path -- Ex: $ex"
                }

  if(bFTIn) debug(s"$name ${if(hadError.nonEmpty) s"ERROR: $hadError" else ""}, FileTransfer: $info")

  val (idMsg, isOriginatingEnd) = if(info.isOriginator){
                                    info = info.copy(isOriginator = false)
                                    (info.xfrMsgID, true)
                                  } else
                                    (info.xfrMsgID, false)

  if(hadError.isEmpty) {
    self ! FTStart()
  } else {
    reportError(false, trans, info, List(hadError), !isOriginatingEnd)
    context.stop(self)
  }

  def receive = {
    case _:FTStart    =>if(bFTInReceive) debug(s"$name got FTStart")
                        // Redirect the inbound messages to ourselves
                        trans.setRedirect(info, self)
                        if(isOriginatingEnd){     // Message to tell the other end to start the output actor
                          val msg = setupMessage(trans, info.copy(isInbound = false), transFTStart, self, false, true)
                          trans.sendMessage(msg)
                        }
    case ack:ACK      => if(bFTInReceive) debug(s"$name ACK: $ack")

    case nak:NAK      =>if(bFTInReceive) debug(s"$name NAK: $nak")
                        reportError(false, trans, info, List(nak.failReason.toString), true, self)
                        context.stop(self)

    case msg:TMessage =>if(bFTInMsg) debug(s"$name Message: ${msg.strShort}")
                        if(msg.isError) {
                          reportError(false, trans, info, msg.getError.errors, false, msg = Some(msg))
                          // Set up so any messages still in-transit are discarded
                          trans.setRedirect(info, trans.getFTDiscard)
                          context.stop(self)
                        } else if(msg.connID==info.request.connID && msg.msgID == info.xfrMsgID && !msg.isChunked)
                          messageStart(msg)
                        else if(msg.connID==info.request.connID && msg.msgID == info.xfrMsgID)
                          messageChunk(msg)
                        else warn(s"$name Unexpected MsgID ignore -- ${msg.strShort}")
  }

  def messageStart(msg:TMessage) = {
    if(msg.isError){
      reportError(false, trans, info, msg.getError.errors, false) // Other end already knows
      trans.setRedirect(info, trans.getFTDiscard)
      context.stop(self)             // Should not be any data Message's since there was an error
    }
  }

  var totalLength = 0L

  def messageChunk(msg:TMessage) = {
    val data = msg.data
    try{
      if(data.length > 0) {
        val bb = ByteBuffer.wrap(msg.data)
        fileChannel.write(bb)
        totalLength += data.length
      }
      if(msg.isFirstChunk) info = info.copy(millisFirstData = System.currentTimeMillis)
      if(msg.isLastChunk){
        fileChannel.close
        if(info.request.markReadOnly){
          path.toFile.setReadOnly
        }
        if(info.request.preserveTimes){
          val fileAttrs = Files.getFileAttributeView(path, classOf[BasicFileAttributeView])
          fileAttrs.setTimes(FileTime.fromMillis(info.millisChanged), FileTime.fromMillis(info.millisLastRef), FileTime.fromMillis(info.millisCreated))
        }
        stopThisEnd(false, StageDone, trans, info)
        context.stop(self)
      }
    } catch {
      case ex:Exception => val err = s"$name failed, Ex: $ex"
                           reportError(false, trans, info, List(name, "Exception reading Chunk", ex.toString), true, self)
                           context.stop(self)
    }
  }
}
object FileTransfer extends JsonSupport {
  import Transport._

  /** Setup a skeleton message with the basics, caller then uses msg.copy(...) to set rest of the fields */
  def setupMessage(trans:Transport, info:FTInfo, name:String, actor:ActorRef, chunked:Boolean, setupInfoAsData:Boolean) =
    TMessage(trans=trans, connID=info.request.connID, msgID=info.xfrMsgID, name =  name, flags = if(chunked) FChunked.flag else 0, ackTo = actor, sentAt = System.currentTimeMillis,
             isPartOfFT = true, data = if(setupInfoAsData) info.toJson.compactPrint.getBytes else Array[Byte](0))

  def reportStage(stage:FTStage, trans:Transport, infoIn:FTInfo):FTInfo = {
    val info = infoIn.copy(stage = stage.code)
    trans.app.transFTStages(trans, info)
    info
  }
  def stopThisEnd(isOutbound:Boolean, stage:FTStage, trans:Transport, infoIn:FTInfo, msg:Option[TMessage] = None):FTInfo = {
    val info = reportStage(stage, trans, infoIn)
    trans.app.transStopTransfer(info)
    trans.dropRedirect(info)
    if(isOutbound) trans.app.transFTOutboundDone(trans, info)
    else           trans.app.transFTInboundDone(trans, info)
    info
  }

  def reportError(isOutbound:Boolean, trans:Transport, infoIn:FTInfo, errors:List[String], notifyOtherEnd:Boolean,
                  fromActor:ActorRef = Actor.noSender, msg:Option[TMessage] = None):FTInfo =
  {
    var info = infoIn.copy(error = Error(infoIn.request.connID, infoIn.xfrMsgID, errors = errors))
    info = stopThisEnd(isOutbound, StageError, trans, info)
    if(notifyOtherEnd) {
      val msgStop = setupMessage(trans, info, Transport.transFTStop, fromActor, false, true)
      trans.sendMessage(msgStop)
    }
    info
  }
}

/****************************************************************************************************/
/** Actor to garbage collect                                                                        */
/****************************************************************************************************/
/** One of these will be created if ANY transfer is initiated for a given Transport.
 *
 *  If any File Transfer fails in the middle, there might be some random data TMessage's somewhere
 *  in transit. The INBOUND side will redirect all messages here just before closing down. This
 *  Actor in turn will just toss them away.
 **/
class FileTransferDiscard(val trans:Transport, val logDiscards:Boolean = false) extends Actor {

  def receive = {
    case msg:TMessage => if(logDiscards) Transport.info(s"FT Discard: ${msg.strShort}")

    case unk          => if(logDiscards) Transport.info(s"FT Disard -- Unknown message: $unk")
  }
}
