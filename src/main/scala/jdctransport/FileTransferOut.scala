package jdctransport

import java.util.UUID
import java.io._
import java.nio._
import java.nio.file._
import java.nio.channels._
import java.util.Arrays
import akka.actor._
import spray.json._

import java.nio.file.attribute.BasicFileAttributes

/**
 * Transfer a file FROM this machine TO the target.
 *
 * @param uuid              - unique UUID assigned for this transfer
 * @param fileAtSource      - absolute path to the source file to be transferred
 * @param fileAtDestination - absolute path to where this file should be written at the target (which may be on the
 *                            same machine)
 * @param msgIDStart        - msg ID of the initial message sent to the recipient with all of the file information
 * @param msgIDXfr          - all chunks in this transfer will have this msgID. On the receiving end, allows the
 *                            receiver to start a FileTransferIn Actor and then vector all work to that actor.
 * @param isGrowing         - TRUE == this file is growing dynamically. Keep reading forever & sending data to target.
 *                            FALSE== snapshot the length at start and send only that amount to the target
 * @param markReadOnly      - receiving end should mark the newly received file as read-only
 * @param preserveTimes     - if true, then the received file should set the times of the target file to match the millis... values
 * @param totalSize         - total data size of the file being transferred (at start, see isGrowing)
 * @param millisCreated     - creation time of the source file
 * @param millisChanged     - last changed time of the source file
 * @param millisLastRef     - last reference (opened) time of the source file
 */
final case class FileTransferInfo(fileAtSource:String, fileAtDestination:String, uuid:String="", msgIDStart:Long = 0, msgIDXfr:Long = 0, isGrowing:Boolean = false, markReadOnly:Boolean=false, preserveTimes:Boolean=true,
                                  totalSize:Long=0, millisCreated:Long=0, millisChanged:Long=0, millisLastRef:Long=0)

/** Passed as constructor of this Actor, and also returned to the 'reportTo' if errors or at EOF */
final case class FileTransfer(connID:Long, outactor:ActorRef, reportTo:ActorRef, info:FileTransferInfo, backPressureLow:Int = Transport.defaultBackpressureLow, backPressureHigh:Int = Transport.defaultBackpressureHigh, xfrStart:Long = 0, xfrEnd:Long = 0, appData:AppData = new AppData(), errors:List[String]=Nil)

private final case class FTStart()
private final case class FTClear()
private final case class FTBackPressure()

private final case class FTXfrN() extends TransportStep

// FIXME -- did not test the 'isGrowing' logic AT ALL, open question whether channel.read will return 0 if there
//          is no more data but some other process is writing to the file, or -1???

/** Generic implementation of a File Transfer through the jdctransport mechanism.
 *
 *  This is in the form of an Actor which:
 *  -- is a one-time-use Actor. It handles a single transfer, then shuts down.
 *  -- has a FileTransfer constructor param to start the transfer
 *  -- verifies existence and readability of the source file
 *  -- sends the file as a 2-step process
 *     -- Step 1 assigns a UUID and sends a Message with the FileTransferInfo to the recipient
 *     -- Step 2 sends the actual file data in chunks the Name in the first chunk
 *               will be FT-<uuid>
 *  -- uses the SendMessage trait to handle throttling
 *  -- sends a FileTransferStart final report to the 'reportTo' Actor
 *  -- terminates
 **/
class FileTransferOut(fileXfrStart:FileTransfer) extends Actor with JsonSupport with SendMessage {
  import FileTransferConst._
  import Transport._

  var fileInfo      = fileXfrStart.info.copy(uuid = UUID.randomUUID.toString)
  var fileXfr       = fileXfrStart.copy(info = fileInfo)
  val connID        = fileXfr.connID

  // The variables needed by SendMessage & DelayFor traits
  val actorContext  = context
  val actorSelf     = self
  val name          = s"FT - ${fileXfr.info.fileAtSource}"    // NOTE: Name of the Actor, not the Message
  val outactor      = fileXfr.outactor

  override
  val minInFlight   = defaultFTBackpressureLow
  override
  val maxInFlight   = defaultFTBackPressureHigh         // NOTE: within THIS logic, also stops reading if > this
                                                        //       number of Messages in the SendMessage Q so that we
                                                        //       do not throw OOM

  val f        = new File(fileXfr.info.fileAtSource)
  val path     = f.toPath
  val hadError = if(!f.exists) {
                    "File does not exist"
                  } else if(!f.canRead) {
                   "File cannot be read"
                  } else if(!f.isFile) {
                    "This is not a File, cannot be transferred"
                  } else
                     ""

  if(hadError.nonEmpty) {
    if(fileXfr.reportTo != Actor.noSender) fileXfr.reportTo ! fileXfr.copy(errors = List(hadError))
    context.stop(self)
  } else
    self ! FTStart()

  lazy val attrs = Files.readAttributes(path, classOf[BasicFileAttributes])
  var nameChunks = ""               // Message name for the 1st chunk sent
  var sz1stChunk = 0                // Amount of data in 1st chunk
  val szNthChunk = maxDataIfNoName  // Max amount of data in all chunks after the 1st (with no name)
  var numChunks  = -1               // -1 == file is dynamic, keep sending data forever as it is present
  var numSent    = 0

  val bfr        = new Array[Byte](maxDataIfNoName) // Input buffer for reads, then copy made for each Message sent
  val bb         = ByteBuffer.wrap(bfr)
  val buffer     = bb.asInstanceOf[Buffer]          // See https://stackoverflow.com/questions/61267495/exception-in-thread-main-java-lang-nosuchmethoderror-java-nio-bytebuffer-flip

  var timeStart     = 0L
  lazy val fChannel = FileChannel.open(path)

  def receive = {
    case _:FTStart        =>fileInfo = fileInfo.copy(totalSize = attrs.size, millisCreated = attrs.creationTime.toMillis, millisChanged = attrs.lastModifiedTime.toMillis, millisLastRef = attrs.lastAccessTime.toMillis)
                            try {
                              fChannel        // force the open so we handle if it throws

                              val json = fileXfr.info.toJson.compactPrint
                              val msg  = setupMessage(fileXfr, ftName)

                              sendMessage(msg.copy(data = json.getBytes))

                              nameChunks = ftXfrName(fileInfo)
                              if(!fileInfo.isGrowing) {
                                sz1stChunk = maxData(nameChunks.getBytes.length)
                                numChunks  = if(attrs.size <= sz1stChunk) 1 else ((attrs.size - sz1stChunk)/szNthChunk).toInt + 1 + 1
                              }
                              timeStart = System.currentTimeMillis
                              self ! FTXfrN()
                            } catch {
                              case ex:Exception =>val errstr = s"Exception opening input File -- Ex: $ex"
                                                  if(fileXfr.reportTo != Actor.noSender) fileXfr.reportTo ! fileXfr.copy(errors = List(errstr))
                                                  val err = Error(connID = fileXfr.connID, msgID=fileXfr.info.msgIDXfr, errors = List(errstr))
                                                  val msg = setupMessage(fileXfr, ftName)
                                                  sendMessage(msg.copy(msgID = fileXfr.info.msgIDStart, flags = flagError, data = err.toJson.compactPrint.getBytes))
                                                  self ! FTClear()
                            }

    case ack:ACK          => if(ack.msg.msgID==fileInfo.msgIDXfr) {
                               sendQueued
                             }

    case nak:NAK          => error(s"Failed transferring File: ${fileInfo.fileAtSource} -- ${nak.failReason}")
                             context.stop(self)

    case _:CheckQueue     => sendQueued             // Handle back-pressure

    case _:FTClear        => if(sendQSize==0) {       // Clear any queued messages, then stop
                                if(fileXfr.reportTo!=Actor.noSender){
                                  fileXfr.reportTo ! fileXfr.copy(xfrStart = timeStart, xfrEnd = System.currentTimeMillis)
                                  context.stop(self)
                                }
                              } else {
                                sendQueued
                                self ! FTClear()      // We're at end, so just spin until Q is empty
                              }

    case _:FTXfrN         =>  buffer.position(0)
                              buffer.limit( if(numSent==0) sz1stChunk else szNthChunk)
                              if(sendQSize > maxInFlight)       // Local back-pressure so we don't blow memory
                                delayFor(FTXfrN())
                              else
                                self ! ReadRemaining()

    case _:ReadRemaining  =>fChannel.read(bb) match {
                              case 0  => delayFor(ReadRemaining())
                              case -1 => val msg = setupMessage(fileXfr)
                                         if(!fileXfr.info.isGrowing){
                                            val data = if(buffer.position() > 0) Arrays.copyOf(bfr, buffer.position()) else new Array[Byte](0)
                                            sendMessage(msg.copy(flags = msg.flags | (if(fileXfr.info.isGrowing) 0 else flagLastChunk), data = data))
                                            self ! FTClear()
                                         } else
                                           self ! FTXfrN()

                              case _ => val isFull = buffer.position() == buffer.limit()
                                        if(isFull || fileXfr.info.isGrowing) {        // For dynamic files, always send whatever we've got
                                          val msg = setupMessage(fileXfr)
                                          sendMessage(msg.copy(data = Arrays.copyOf(bfr, buffer.position)))
                                          self ! FTXfrN()
                                        } else
                                          self ! ReadRemaining()
                            }

    case unk              => Transport.warn(s"FileTransferOut did not recognize $unk")
  }
  // Setup a Message skeleton as CHUNKED - then use msg.copy(...) to fill in as needed
  def setupMessage(xfr:FileTransfer, name:String="") = Message(connID=fileXfr.connID, msgID=fileXfr.info.msgIDStart, name =  name, flags = flagChunked, ackTo = self)
}

object FileTransferConst {
  val ftName    = "jdctransport.FileTransfer"      // Name of the initial message with all the File Transfer info
  val ftChunks  = "jdctransport.FileXfr"         // Name of 1st message with chunks with UUID appended

  def ftXfrName(info:FileTransferInfo) = s"$ftChunks--${info.uuid}"
}

