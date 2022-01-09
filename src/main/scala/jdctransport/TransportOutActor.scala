package jdctransport

import java.nio.ByteBuffer
import java.util.{Arrays, ArrayDeque}
import akka.actor.{Actor, ActorRef}
import scala.collection.mutable

/** Application sends output Message's to this Actor, which will chunk them if necessary.
 *  Note: Large data transfers should be chunked by the Application so the whole thing is not in memory!
 *  ACK is sent once each message or chunk is passed to the OutputStream
 */
class TransportOutActor(trans:Transport ) extends Actor with DelayFor {
  import Transport._

  val actorContext= context       // For the SleepFor trait
  val actorSelf   = self

  val nameBase:String = s"   TransportOUT ${trans.channelID}: "
  var name            = nameBase
  val connections     = mutable.Map.empty[Int, ActorRef]     // connID -> application actor


  val queue           = new ArrayDeque[TMessage]()        // Queue of Message's to be sent. Need to keep this
  // since new Message's can arrive while we're still
  // writing earlier ones.

  var nullCycles      = 0          // Count of null write cycles (nothing to write)

  // Variables while writing one segment - either a complete Message or a Chunk
  var message:TMessage   = null
  var bb:ByteBuffer      = null
  var length             = 0
  var amtWritten         = 0

  if(bTransportOpen) debug(f"$name STARTING --")
  self ! WriteInitial()     // To get the process started

  def receive = {

    case conn:StartConn => startConn(name, conn, trans)

    case _:StartClient  => if(bTransportOutbound) debug(s"$name -- received StartClient")
    // no-op at this time

    case cls:Close     => trans.close(cls.connID)

    case msg:TMessage   =>if(bTransportOutbound) {
                            name = s"$nameBase ${msg.name}"
                            debug(s"$name Outbound Message -- ${msg.strShort}")
                          }
                          if(!msg.isValid)
                            sender ! NAK(msg.connID, NAK.notValid, msg)
                          else if (msg.length > Transport.maxMessageSize)
                            sendChunks(msg)
                          else {
                            queue.add(msg)
                          }

    case w:WriteInitial =>message = queue.pollFirst
                          if(bTransportOutbound) {
                            if(message==null) {
                              name = nameBase
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
                            bb     = ByteBuffer.wrap(message.asBuffer(trans))
                            doTheWrite
                          }
    case w:WriteMore    =>doTheWrite

    case unk           => warn(s"$name -- UNKNOWN skipped -- $unk")
  }

  def doTheWrite = {
    val n = { try {
      trans.channel.write(bb)
    } catch {
      case _:Exception => self ! Close(connIDCloseAll)
        0
    }
    }
    amtWritten += n
    if(bTransportOutDtl) { if(n > 0) debug(f"$name -- doWrite N: $n%,d, AmtWritten: $amtWritten%,d, Length: $length%,d") }
    if(amtWritten==length){
      trans.inFlight.decrementAndGet
      if( message.isPartOfFT==false || trans.app.transSuppressFileTransferToMsg==false )
        trans.app.transMsgSent(message, trans)
      if(message.ackTo != Actor.noSender) message.ackTo ! ACK(message.connID, message)
      delayReset
      if(!message.isChunked || message.isLastChunk) {
        if(bAppSent) debug(s"$name -- calling transMsgSent ${message.strShort}")
        trans.app.transMsgSent(message, trans)
      }
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
  private def sendChunks(msg:TMessage) = {
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
      trans.inFlight.addAndGet(numChunks - 1)       // ASSUME that the sender added 1 for the overall Message
      for(n <- 1 to numChunks){
        val sendMsg = if(n == 1) {
          val rslt = msg.copy(flags = msg.flags | FChunked.flag | FDeChunk.flag | FFirstChunk.flag, data = Arrays.copyOfRange(msg.data, 0, maxData1st), appData = Some(appData))
          queue.add(rslt)
          rslt
        } else {
          val fullTo = dataOffset + maxDataNth
          val dataTo = if(fullTo > msg.data.length) msg.data.length else fullTo
          appData    = appData.copy(sentSoFar = dataTo, numThisChunk = n)
          val rslt = msg.copy(flags = msg.flags | FChunked.flag | FDeChunk.flag | (if(n == numChunks) FLastChunk.flag else 0), name="", data = Arrays.copyOfRange(msg.data, dataOffset, dataTo), appData = Some(appData))
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
