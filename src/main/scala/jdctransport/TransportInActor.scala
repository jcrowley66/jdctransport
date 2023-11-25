package jdctransport

import akka.actor.{Actor, Props}
import java.nio.{Buffer, ByteBuffer}
import spray.json._

/** Actor to handle the input side of the channel. Will send Message messages to the Application.
 *  Infinite loop reads the input, sending messages to 'self' so Actor never blocks.
 **/
class TransportInActor(val trans:Transport) extends TransActor with DelayFor with JsonSupport {
  import Transport._

  implicit val exec = context.dispatcher

  val actorContext= context       // For the SleepFor trait
  val actorSelf   = self

  val nameBase    = f"    TransportIN ${trans.transID}/${trans.channelID}%,d:"
  var name        = nameBase

  var buffer:ByteBuffer   = null        // Allocate & sometimes re-use max size buffer for normal messages
                                        // Bug in some of the ByteBuffer methods - position(), etc so must use them
                                        // in the basic Buffer object. Especially if compiling in Java 8!
                                        // See https://stackoverflow.com/questions/61267495/exception-in-thread-main-java-lang-nosuchmethoderror-java-nio-bytebuffer-flip

  var bigBfr:Array[Byte]  = null            // Large buffer allocated if we need to de-chunk, then released
  var bigAmtRead          = 0               // How much data is already in the bigBfr

  var readCycleStarted    = false

  var readBaseCycles      = 0

  // Variables while reading one segment - either a complete Message or a Chunk
  var length             = 0

  if(bTransportOpen) debug(s"ACTOR $name STARTING $self ")

  def receive = {
    case conn:StartConn =>if(bTransportRdCycle) debug(s"$name -- StartConn message")
                          startConn(name, conn, trans)
                          if(!readCycleStarted)
                            self ! ReadBaseInfo()

    case clt:StartClient=>if(bTransportInbound) debug(s"$name -- StartClient message")
                          if(!readCycleStarted) {
                            readCycleStarted = true
                            trans.connections += (0 -> (false, clt.app) )
                            self ! ReadBaseInfo()
                          }

    case cls:Close      =>// ASSUME that the Close was also sent to TransportOutActor & it handles most logic
                          if(cls.connID == connIDAll) trans._inIsClosed = true

    // Several steps possible in the read cycle. First get at least the 'length'.
    case r:ReadBaseInfo =>if(bTransportRdCycle) debug(s"$name -- ReadBaseInfo message")
                          readCycleStarted = true
                          // Start reading one segment - a chunk/message. Read at least the base info if available
                          resetAll
                          if(buffer == null) {
                            buffer  = ByteBuffer.allocate(Transport.maxMessageSize)
                          }
                          buffer.asInstanceOf[Buffer].position(0)
                          buffer.asInstanceOf[Buffer].limit(szBaseInfo)                // Base info must exist in every message, so try to read
                                                                  // Note: Some messages have ONLY the base info (flags control)
                          channelRead
                          var pos = buffer.asInstanceOf[Buffer].position()

                          if(pos >= szLength) {                   // Got at least the length, so set the final limit
                            length = buffer.getInt(0)
                            buffer.asInstanceOf[Buffer].limit(length)

                            if (pos < length) {                   // See if we can read the rest of the message
                              if (channelRead > 0)                // right away without another cycle thru this Actor
                                pos = buffer.asInstanceOf[Buffer].position()
                            }
                          }

                          if(length == 0)                           // Still haven't been able to even read the length
                                                                    // but might have read a couple of bytes
                            delayFor(ReadBaseMore())                // ... so wait a bit, then try again
                          else if(pos == length){                   // Got lucky!
                            processSegment
                            self ! ReadBaseInfo()
                          } else
                            self ! ReadRemaining()

    case r:ReadBaseMore =>if(bTransportRdCycle) {
                            if(buffer.asInstanceOf[Buffer].position() > 0) debug(s"$name -- ReadBaseMore message, length: $length, BufferPos: ${buffer.asInstanceOf[Buffer].position()}, Limit: ${buffer.asInstanceOf[Buffer].limit()}")
                          }

                          channelRead

                          if(buffer.asInstanceOf[Buffer].position() >= szLength) {     // At least got the length
                            length = buffer.getInt(0)
                            if(length <0 || length > maxMessageSize){
                              error(s"Read invalid length $length, shutting Transport down")
                              self ! Close(Transport.connIDAll)
                            } else {
                              buffer.asInstanceOf[Buffer].limit(length)
                              self ! ReadRemaining()
                            }
                          } else {
                            delayFor(ReadBaseMore())              // otherwise wait a bit, then try to get rest of length again
                          }

    case r:ReadRemaining=>if(bTransportRdCycle) debug(s"$name -- ReadRemaining message, length: $length, BufferPos: ${buffer.asInstanceOf[Buffer].position()}, Limit: ${buffer.asInstanceOf[Buffer].limit()}")
                          // Have at least the length+, read the remaining data for one Chunk/Message -- IFF any remaining
                          var n = 1
                          while(n > 0) {        // Keep cycling as long as we read at least some data
                            n = 0               // drops out of this cycle if we don't ready any at all
                            var pos = buffer.asInstanceOf[Buffer].position()
                            if (pos < length) {
                              n = channelRead             // try to read more data
                              if(n > 0) pos = buffer.asInstanceOf[Buffer].position()
                            }
                            if (pos == length) {
                              processSegment
                              self ! ReadBaseInfo()
                              n = 0                       // to drop out of this loop
                            } else {
                              if(n==0) delayFor(ReadRemaining())    // Delay if did not read anything at all (and n==0 exits this loop)
                            }
                          }

    case ack:ACK         => // no-op
    case nak:NAK         => // no-op

    case unk             => warn(s"$name -- UNKNOWN skipped -- $unk")
  }
  private def channelRead:Int =
    if(length == 0 || buffer.asInstanceOf[Buffer].position() < length) {
      try {
        val n = buffer.synchronized{ trans.channel.read(buffer) }
        if(n > 0) delayReset
        n
      } catch {
        case ex:Exception =>trans.closeAll(clearOutQ = true, ex = Some(ex) )
                            return -1
                            0
      }
    } else
      0

  // 'buffer' contains a complete Chunk or Message, decide what to do with it
  def processSegment:Unit = {
    buffer.asInstanceOf[Buffer].position(0)
    buffer.asInstanceOf[Buffer].limit(length)
    // If large enough, just grab the whole buffer otherwise allocate smaller one & copy over
    val usebb = if(length > Transport.maxMessageSize / 4) {
                  val tmp = buffer
                  buffer  = null
                  tmp
                } else {
                  val bbnew = ByteBuffer.allocate(length)
                  bbnew.put(buffer)
                  bbnew.asInstanceOf[Buffer].position(0)
                  bbnew.asInstanceOf[Buffer].limit(length)
                  bbnew
               }
    val seg = OTWMessage(usebb, trans)

    if (bTransportSegment) debug(s"$name OTW INBOUND SEGMENT: ${seg.strShort}")
    if( bTransportBrkPt ) transBrk(seg.name)

    trans.connections.get(seg.connID) match {
      case None               => // Get this situation regularly so don't issue a message
                                 //warn(s"ConnID: ${seg.connID} not found as an active Connection")
      case Some((closed, _))  => if(closed) return
    }

    if( ! seg.mustDeChunk ){         // We don't have to de-chunk, just send the msg to the App
      seg.setFlags(TFInbound)
      sendToApp(seg, false)
    } else {
      if(seg.isFirstChunk && seg.isLastChunk){  // Handle aberration - de-chunk & both first & last chunks in one segment
        seg.setFlags(TFInbound)
        sendToApp(seg, true)
      } else if(bigBfr==null) {
        bigBfr = new Array[Byte](Transport.maxMessageSize * 12)           // Guess on size, expand if needed
        System.arraycopy(seg.asBuffer, 0, bigBfr, 0, seg.length)  // copy the first segment
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
          val otw = OTWMessage(bbBig, trans)
          otw.clearFlags(TFAnyChunk.flag)
          otw.setFlags(TFInbound)
          if (bTransportDeChunk) debug(s"$name OTW INBOUND LAST CHUNK: ${otw.strShort}")
          sendToApp(otw, true)
          bigBfr = null
        }
      }
    }
  }

  def sendToApp(otw:OTWMessage, hasBeenDeChunked:Boolean = true) = {
    if(bInboundFunc){
      if(inboundTest(otw))
        inboundFunc(otw)
    }
    if(hasBeenDeChunked) otw.clearFlags(TFAnyChunk)
    if(bTransportTrace) debugTrace("TransInActor", otw, trans)
    val connID = otw.connID
    if (bTransportInbound) debug(s"$name -- INBOUND MSG: ${otw.strShort}")
    if (connID == connIDBroadcast) {
      trans.connections.filterNot {  case(key, _) => key == 0 }.foreach{ case(_, (closed, app) ) =>
        if(bAppReceived) debug(s"transMsgRecieved -- ${otw.strShort}")
        app.transMsgReceived(traceProcess(trans, TraceRcv, otw), trans)
      }
    } else trans.redirects.get(otw.msgKey) match {
      case Some(ref) => ref ! otw
      case None      =>
        if(otw.isConnCreated) {                   // Special message which just defines the ConnID for this application/user
          if(trans.connections.contains(0))
            trans.connections -= 0
          else {
            // Happens often & is somewhat normal so suppress the warning
            // warn(s"msgConnectionCreated received, but no ZERO-entry was found, ${otw.toShort}")
          }
          val app = trans.app
          trans.connections += (otw.connID -> (false, app) )
          if (bTransportInbound) debug(s"$name -- sending AppConnect to $app")
          if (bAppConnOpen) debug(s"$name -- got isConnCreated. Added to connections and calling transConnOpen ConnID: $connID")
          app.transConnOpen(connID, trans)
        } else if(otw.isFTStart) {                // Other end wants to start a file transfer
          var info = new String(otw.data).parseJson.convertTo[FTInfo]
          val (ftOK, appDataOpt) = trans.app.transAllowFT(info)
          info = updateInfo(info, appDataOpt)
          if (ftOK) { //} && !trans.app.transFTStartHandled(info, otw)) {
            if(!trans.app.transFTStartHandled(info, otw)) {
              if (info.isInbound) {
                trans.actorSystem.actorOf(Props(new FileTransferIn(trans, info)))
              } else
                trans.actorSystem.actorOf(Props(new FileTransferOut(trans, info)))
            }
          } else {
            val msgOut = TMessage(trans = trans, connID = connID, msgID = info.xfrMsgID, name = "", data = otw.data, flags = TFTRefused.flag)
            trans.sendMessage(msgOut)
          }
        } else if(connID==0)
            error(f"$name -- ConnID is ZERO -- $otw")
         else {
          trans.connections.get(connID) match {
            case Some((closed, app)) => if (bTransportInbound) debug(s"$name -- INBOUND MSG -- Calling transMsgReceived: ${otw.strShort}")
                                        app.transMsgReceived(traceProcess(trans, TraceRcv, otw), trans)
            case None                => if (bTransportInbound) debug(s"$name -- INBOUND MSG -- Calling transMsgReceived: ${otw.strShort}")
                                        warn(f"$name -- ConnID: $connID%,d is not defined -- using default, Msg: ${otw.strShort}")
                                        trans.connections += (connID -> (false, trans.app) )
                                        trans.app.transMsgReceived(traceProcess(trans, TraceRcv, otw), trans)
          }
          if(bTransportDelayIn) threadSleep(nTransportDelayIn)
        }
    }
  }

  def resetAll = {
    length          = 0
    readBaseCycles  = 0
    delayReset
  }
}
