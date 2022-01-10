package jdctransport

import akka.actor.{Actor, Props}
import java.nio.{Buffer, ByteBuffer}
import spray.json._

/** Actor to handle the input side of the channel. Will send Message messages to the Application.
 *  Infinite loop reads the input, sending messages to 'self' so Actor never blocks.
 **/
class TransportInActor(trans:Transport) extends Actor with DelayFor with JsonSupport {
  import Transport._

  implicit val exec = context.dispatcher

  val actorContext= context       // For the SleepFor trait
  val actorSelf   = self

  val nameBase    = f"    TransportIN ID: ${trans.channelID}%,d"
  var name        = nameBase

  val bfr         = new Array[Byte](Transport.maxMessageSize) // Allocate & re-use max size buffer for normal messages
  val bb          = ByteBuffer.wrap(bfr)                      // Channel operations use the ByteBuffer position & limit
  val buffer      = bb.asInstanceOf[Buffer]                   // Bug in some of the ByteBuffer methods - position(), etc so must use them
                                                              // in the basic Buffer object.
                                                              // See https://stackoverflow.com/questions/61267495/exception-in-thread-main-java-lang-nosuchmethoderror-java-nio-bytebuffer-flip

  var bigBfr:Array[Byte] = null             // Large buffer allocated if we need to de-chunk, then released
  var bigAmtRead         = 0                // How much data is already in the bigBfr

  var readCycleStarted   = false

  var readBaseCycles     = 0

  // Variables while reading one segment - either a complete Message or a Chunk
  var length             = 0

  if(bTransportOpen) debug(f"$name STARTING $self ")

  def receive = {
    case conn:StartConn =>startConn(name, conn, trans)
                          if(!readCycleStarted)
                            self ! ReadBaseInfo()

    case clt:StartClient=>if(bTransportInbound) debug(s"$name -- received StartClient")
                          if(!readCycleStarted) {
                            readCycleStarted = true
                            trans.connections += (0 -> clt.app)
                            self ! ReadBaseInfo()
                          }

    case cls:Close      =>if(cls.connID==connIDCloseAll) {
                            try {trans.channel.close } catch {case _: Exception => /* IGNORED */ }
                            context.stop(self)
                          } else trans.connections.get(cls.connID) match {
                            case None    => warn(s"$name -- Close for ${cls.connID}, but not found in map")
                            case Some(_) => trans.connections -= cls.connID
                          }

    // Several steps possible in the read cycle. First get all the in-the-clear info, mainly so we have the 'length'
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
                              debug(s"$name -- ReadBaseMore Cycles: $readBaseCycles, Position: ${buffer.position()}, Length: $length ")
                          }
                          val n = channelRead
                          if(length==0 && buffer.position() >= szLength) {
                            length = bb.getInt(0)
                            buffer.limit(length)
                          }
                          if(buffer.position() >= szBaseInfo) {
                            if(buffer.position() == length){
                              processSegment
                              self ! ReadBaseInfo()
                            } else {
                              readBaseCycles = 0
                              self ! ReadRemaining()
                            }
                          } else if(n > 0)
                            self ! ReadBaseMore()
                          else
                            delayFor(ReadBaseMore())


    case r:ReadRemaining=>// Read the remaining data for one Chunk/Message -- IFF any remaining
                          val n = if(buffer.position() < length)
                                    channelRead
                                  else
                                    0
                          if(bTransportRdCycle) {
                            readBaseCycles += 1
                            if (nEmptyReadCycles > 0 && (readBaseCycles % nEmptyReadCycles) == 0)
                              debug(s"$name -- ReadRemaining Position: ${buffer.position()}, Limit: ${buffer.limit}, Length: $length, LastRd: $n ")
                          }
                          if(buffer.position() == length) {
                            processSegment
                            self ! ReadBaseInfo()
                          } else if(n > 0)
                            self ! ReadRemaining()
                          else
                            delayFor(ReadRemaining())

    case unk             => warn(s"$name -- UNKNOWN skipped -- $unk")
  }
  def channelRead:Int =
    if(length == 0 || buffer.position() < length) {
      try {
        val n = trans.channel.read(bb)
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
    if (bTransportSegment) debug(s"$name OTW INBOUND SEGMENT")
    val seg = OnTheWireBuffer(bb, trans).asMessage       // Will decrypt if necessary
    if (bTransportSegment) debug(s"$name OTW INBOUND SEGMENT: ${seg.strShort}")
    if(!seg.isDeChunk){         // We don't have to de-chunk, just send the msg to the App
      sendToApp(seg)
    } else {
      if(seg.isFirstChunk && seg.isLastChunk){  // Handle aberration - de-chunk & both first & last chunks in one segment
        sendToApp(seg)
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
          val otw = OnTheWireBuffer(bbBig, trans)
          otw.setNoChunks
          val sendMsg = otw.asMessage
          if (bTransportDeChunk) debug(s"$name OTW INBOUND LAST CHUNK: ${sendMsg.strShort}")
          sendToApp(sendMsg)
          bigBfr = null
        }
      }
    }
  }

  def sendToApp(msg:TMessage) = {
    val connID = msg.connID
    if (bTransportInbound) debug(s"$name -- INBOUND MSG: ${msg.strShort}")
    if (connID == connIDBroadcast) {
      trans.connections.filterNot {  case(key, value) => key == 0 }.foreach{ case(_, app) =>
        if(bAppReceived) debug(s"transMsgRecieved -- ${msg.strShort}")
        app.transMsgReceived(msg, trans)
      }
    } else trans.redirects.get(msg.msgKey) match {
      case Some(ref) => ref ! msg
      case None      => msg.name match {      // Some messages are handled by this Transport itself, most just passed to the Application
                          case Transport.transConnectionCreated => trans.connections.get(0) match {     // Will have a ZERO entry if starting a Client
                                                                      case None      => error(f"msgConnectionCreated received, but no ZERO-entry has been defined")
                                                                      case Some(app) =>
                                                                        trans.connections -= 0
                                                                        trans.connections += (msg.connID -> trans.app)
                                                                        if (bTransportInbound) debug(s"$name -- sending AppConnect to $app")
                                                                        if(bAppConnOpen) debug(s"$name -- calling transConnOpen ConnID: $connID")
                                                                        trans.app.transConnOpen(connID, trans)
                                                                   }
                          case Transport.transFTStart           => // Other end wants to start a file transfer
                                                                  var info = new String(msg.data).parseJson.convertTo[FTInfo]
                                                                  val (ftOK, appDataOpt) = trans.app.transAllowFT(info)
                                                                  info = updateInfo(info, appDataOpt)
                                                                  if(ftOK) {
                                                                    if(info.isInbound) trans.actorSystem.actorOf(Props(new FileTransferIn(trans, info)))
                                                                    else               trans.actorSystem.actorOf(Props(new FileTransferOut(trans, info)))
                                                                  } else {
                                                                    val msgOut = TMessage(trans = trans, connID=connID, msgID = info.xfrMsgID, name = Transport.transFTRefused, data = msg.data)
                                                                    trans.sendMessage(msgOut)
                                                                  }
                          case other                            => if(connID==0)
                                                                      error(f"$name -- ConnID is ZERO -- $msg")
                                                                   else trans.connections.get(connID) match {
                                                                      case Some(app) => if (bTransportInbound) debug(s"$name -- INBOUND MSG -- Calling transMsgReceived: ${msg.strShort}")
                                                                                        app.transMsgReceived(msg, trans)
                                                                      case None      => warn(f"$name -- ConnID: $connID%,d is not defined -- using default, Msg: ${msg.strShort}")
                                                                                        trans.connections += (msg.connID -> trans.app)
                                                                                        trans.app.transMsgReceived(msg, trans)
                                                                   }
                        }
    }
  }

  def resetAll = {
    length          = 0
    readBaseCycles  = 0
    delayReset
  }
}
