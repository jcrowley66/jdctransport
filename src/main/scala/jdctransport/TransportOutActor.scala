package jdctransport

import java.nio.ByteBuffer
import java.util.Arrays
import java.util.concurrent.ConcurrentLinkedDeque
import akka.actor.{Actor, ActorRef}
import scala.collection.mutable
import scala.collection.JavaConverters._

/** If a sender has an Actor to which a response should be returned */
case class TranOutWithRef(otw:OTWMessage, respondTo:ActorRef)

/** Application sends output Message's to this Actor, which will chunk them if necessary.
 *  Note: Large data transfers should be chunked by the Application so the whole thing is not in memory!
 *  ACK is sent once each message or chunk is passed to the OutputStream
 */
class TransportOutActor(val trans:Transport ) extends TransActor with DelayFor {
  import Transport._

  val actorContext= context       // For the SleepFor trait
  val actorSelf   = self

  val nameBase:String = s"   TransportOUT ${trans.transID}/${trans.channelID}: "
  val origTarget      = target
  def target:String   = {
                          val addr = trans.channel.getRemoteAddress
                          val str  = if(addr==null) "NoRmtAddr" else addr.toString
                          str
                        }
  var name            = nameBase
  private val queue   = new ConcurrentLinkedDeque[TranOutWithRef]()   // Queue of Message's to be sent. Need to keep this
                                                                      // since new Message's can arrive while we're still
                                                                      // writing earlier ones.
                                                                      // NOTE: Message remains in the Q while being written,
                                                                      //       removed once completely written to channel.

  var nullCycles      = 0          // Count of null write cycles (nothing to write)

  // Variables while writing one segment - either a complete Message or a Chunk
  var message:OTWMessage = null
  var bb:ByteBuffer           = null
  var length                  = 0
  var amtWritten              = 0
  var respondTo               = Actor.noSender

  if(bTransportOpen) debug(s"ACTOR $name STARTING -- $self")
  self ! WriteInitial()     // To get the process started

  def receive = {

    case conn:StartConn      => startConn(name, conn, trans)

    case _:StartClient       => if(bTransportOutbound) debug(s"$name -- received StartClient")
                                // no-op at this time

    case cls:Close           => val qList = queue.iterator.asScala.toList
                                if(cls.connID == connIDAll){
                                  trans.connections.foreach{ case(connID, (closed, transapp) ) => if(!closed) trans.connections += (connID -> (true, transapp) ) }
                                } else trans.connections.get(cls.connID) match {
                                  case None                     => error(s"Got a Close for connID: ${cls.connID}, but not found as active connection")
                                  case Some((closed, transapp)) => if(!closed) trans.connections += (cls.connID -> (true, transapp) )
                                }
                                if(cls.clearOutQ) {                 // Clear any pending entries for this connID (or All)
                                  if(cls.connID == connIDAll) {
                                    queue.clear
                                  } else {
                                    qList.filter( _.otw.connID == cls.connID ).foreach( queue.remove( _ ) )
                                  }
                                  trans.app.transClosed(cls.connID, trans)
                                  if(cls.andShutdown) {
                                    trans._outIsClosed = true
                                    trans.shutdownTrans(wait = true)        // ASSUMEs that app also sent a Close to TransportInActor
                                  }
                                } else {                                                    // Process any pending entries, then close this connID (or All)
                                  if((cls.connID==connIDAll && queue.isEmpty) || !qList.exists( it => it.otw.connID == cls.connID)) {
                                    trans.app.transClosed(cls.connID, trans)
                                    if(cls.andShutdown) {
                                      trans._outIsClosed = true
                                      trans.shutdownTrans(wait = true)      // ditto
                                    }
                                  } else
                                    delayFor(cls)                           // Process the queue & then process the Close message again
                                }

    case msg:TransMessage    => doTheSend(msg.asOTW, sender)
    case ref:TranOutWithRef  => doTheSend(ref.otw, ref.respondTo)

    case w:WriteInitial      => val tranref = queue.peekFirst
                                if(bTransportOutDtl) {
                                  if(tranref==null) {
                                    name = nameBase
                                    nullCycles += 1
                                    if(nNullWriteCycles > 0 && (nullCycles % nNullWriteCycles== 0))
                                      debug(f"$name - $nullCycles%,d empty WRITE cycles")
                                  } else {
                                    debug(s"$name - WRITING ${tranref.otw.strShort}")
                                    nullCycles = 0
                                  }
                                }
                                if(tranref==null)
                                  delayFor(WriteInitial())
                                else {
                                  resetAll
                                  respondTo = tranref.respondTo
                                  message   = tranref.otw
                                  length    = message.length
                                  bb        = message.bb
                                  doTheWrite
                                }

    case w:WriteMore         => doTheWrite

    case ack:ACK             => // no-op
    case nak:NAK             => // no-op

    case unk                 => warn(s"$name -- UNKNOWN skipped -- $unk")
  }

  private def doTheSend(otw:OTWMessage, respondTo:ActorRef):Unit = {
    Transport.traceProcess(trans, TraceFT, otw)
    if(bOutboundFunc){
      if(outboundTest(otw))
        outboundFunc(otw)
    }
    if (bTransportTrace) debugTrace("TransOutActor", otw, trans)
    if (bTransportOutbound) {
      val nm = s"$nameBase Orig: $origTarget, Now: $target"
      debug(s"$nm, Message -- ${otw.strShort}")
    }
    trans.connections.get(otw.connID) match {
      case None              => if(otw.isConnCreated)      // This is the message defining the ConnID
                                  trans.connections += (otw.connID -> (false, trans.app) )
                                else {
                                  warn(s"TransInActor.doTheSend -- ConnID: ${otw.connID} not found in active connections")
                                  return
                                }

      case Some((closed, _)) => if(closed) {
                                  warn(s"ConnID ${otw.connID} was closed, ignoring ${otw.toShort}")
                                  respondTo ! NAK(otw.connID, NAK.connIDClosed, otw)
                                  return
                                }
    }

    val length = otw.length
    otw.clearFlags(TFInbound)
    // See if we need to chunk this
    if (length > Transport.maxMessageSize) {
      // Adjust the counters since they were bumped before sending msg here
      // Each chunk will be considered another message within sendChunks
      trans.outboundCnt.decrementAndGet
      trans.outboundData.addAndGet(-length)
      Transport.ttlMessages.decrementAndGet
      Transport.ttlData.addAndGet(-length)
      sendChunks(otw.asTMessage, respondTo)
    } else {
      if (!otw.isValid) {
        if (respondTo == Actor.noSender || respondTo.toString.endsWith("deadLetter]"))
          Transport.error(s"NAK for msg ConnID: ${otw.connID}, Length: ${otw.length}, Flags: XXX, Name: ${otw.name}")
        else
          respondTo ! NAK(otw.connID, NAK.notValid, otw.asTMessage)
      } else {
        trans.inFlight.incrementAndGet
        queue.add(TranOutWithRef(otw, respondTo))
      }
    }
  }

  private def doTheWrite:Unit = {
    val n = { try {
              bb.synchronized{ trans.channel.write(bb) }
            } catch {
              case ex:Exception =>  trans.closeAll(clearOutQ = true, ex = Some(ex) )
                                    return
                                    0
            }
          }
    amtWritten += n
    if(bTransportOutDtl) { if(n > 0) debug(f"$name -- doWrite N: $n%,d, AmtWritten: $amtWritten%,d, Length: $length%,d") }
    if(amtWritten==length){
      queue.removeFirst
      trans.outboundCnt.decrementAndGet
      trans.outboundData.addAndGet(-length)
      Transport.ttlMessages.decrementAndGet
      Transport.ttlData.addAndGet(-length)
      trans.inFlight.decrementAndGet
      if(! (respondTo == Actor.noSender || respondTo.toString.endsWith("deadLetters]")) ) {
        if(bTransportOutbound) debug(s"ACK sent to $respondTo")
        respondTo ! ACK(message.connID, message.asTMessage)
      }
      delayReset
      if((!message.isChunked || message.isLastChunk) && message.isFTSegment==false) {
        if(bAppSent) debug(s"$name -- calling transMsgSent ${message.strShort}")
        trans.app.transMsgSent(traceProcess(trans, TraceSent, message).asOTW, trans)
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
  private def sendChunks(msgIn:TransMessage, respondTo:ActorRef) = {
    val msg = msgIn.asTMessage
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
      val numChunks   = (totalLess1st/maxDataNth) + 1 +                 // + 1 for the 1st chunk
                        (if((totalLess1st % maxDataNth) == 0) 0 else 1) // + 1 for last piece unless an even fit

      var appData     = AppData(totalData = totalData, sentSoFar = maxData1st, numChunks=numChunks, numThisChunk = 1)
      var dataOffset  = 0
      msg.clearFlags(TFAnyChunk)
      for(n <- 1 to numChunks){
        val sendMsg = if(n == 1) {
                        msg.copy(flags = msg.flags | TFChunked.flag | TFDeChunk.flag | TFFirstChunk.flag, data = Arrays.copyOfRange(msg.data, 0, maxData1st), appData = Some(appData))
                      } else {
                        val fullTo = dataOffset + maxDataNth
                        val dataTo = if(fullTo > msg.data.length) msg.data.length else fullTo
                        appData    = appData.copy(sentSoFar = dataTo, numThisChunk = n)
                        msg.copy(flags = msg.flags | TFChunked.flag | TFDeChunk.flag | (if(n == numChunks) TFLastChunk.flag else 0), name="", data = Arrays.copyOfRange(msg.data, dataOffset, dataTo), appData = Some(appData))
                      }
        if(bTransportDoChunk)debug(f"$name -- DO CHUNK -- Max1st: $maxData1st%,d, MaxNth: $maxDataNth%,d, Total: $totalData%,d, NumChunks: $numChunks ${sendMsg.strShort}")
        trans.outboundCnt.incrementAndGet
        trans.outboundData.addAndGet(sendMsg.length)
        Transport.ttlMessages.decrementAndGet
        Transport.ttlData.addAndGet(sendMsg.length)
        trans.inFlight.incrementAndGet
        queue.add(TranOutWithRef(sendMsg.asOTW, respondTo))
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
