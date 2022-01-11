package jdctransport

import java.util.Arrays
import akka.actor.{Actor, ActorRef}
import java.nio.{ByteOrder, ByteBuffer}
import spray.json._

/** Optional data which exists only within the sender - it does not go over-the-wire -- But will be included in ACK & NACK messages
 *  Whether this data is populated and/or used is completely under the control of the application
 *  SUGGESTED data & meaning
 *  NOTE: If include the 'fullMessage' and 'otherData' fields, spray-json cannot automatically provide to/from Json conversion
 */

case class AppData( //fullMessage:Option[Message]=None,     // Recursive link to the complete Message.
                                                          // If data is chunked, these can be useful. Especially useful in ACKs so that
                                                          // other Senders can determine if sending should be throttled until the queue is reduced
                    totalData:Long=0,                     // Total data
                    sentSoFar:Long=0,                     // Total data sent so far
                    numChunks:Int=0,                      // Number of chunks
                    numThisChunk:Int=0,                   // Number of this chunk - 1-based

                    //otherData:Option[AnyRef] = None       // Catch all if Application needs additional information
                  )
/** An original or expanded Message -- all of the numeric fields are larger to hold the unsigned number from the ByteBuffer
 *  The 'asBuffer' method converts to an Array[Byte] form for transmission. See OnTheWireBuffer to parse an inbound buffer and
 *  produce a Message.
 *
 *  Keys
 *  ====
 *  The transID (16 bits) is unique within all Transports within a JVM, and is stored when a TMessage is created. Within a
 *  JVM which receives a TMessage it is meaningless, but the transOrigKey can be used to identify the original TMessage
 *  if sending a response back to an originator.
 *
 *  connID (16 bits) is unique within a Transport and should be an agreed value between two entities using Transport
 *  to send messages to each other.
 *
 *  msgID (32 bits) is assigned by whichever end creates the TMessage, and the Application determines any rules.
 */
case class TMessage (
  trans    :Transport = null,                 // Transport with which associated if CREATING a new TMessage
  // 'length', 'szName' are computed below
  flags    :Long    = 0,                      // Bit flags for certain situations - see Transport.flag... values
  transID  :Int     = 0,                      // TransID which originally creates the TMessage (unique within creating JVM)
  connID   :Int     = 0,                      // ID of the logical Connection to which this message belongs.
  msgID    :Long    = 0,                      // ID of this message. Note: For chunked messages, ID will be the same for all chunks
  appMsgID :Long    = 0,                      // Optional: Application level message ID. NOTE: As UNSIGNED INT
  name     :String  = "",                     // KEY: Defines the type of message. Could be a class name (e.g. mypackage.MyClass)
                                              //      The sender & receiver must decide on all of the 'name' values and the format of the 'data'
                                              //      for a message of this name. E.g. based on the 'name', the data might be a String,
                                              //      JSON, Java object serialization, etc.
                                              //      NOTE: String.length may NOT be the same as the Array[Byte] length if any non-Ascii
                                              //            characters appear in the name
  data:Array[Byte]  = new Array[Byte](0),     // The actual data. For normal messages data.length == length - sizeBaseInfo - name.getBytes.length

  ackTo:ActorRef    = Actor.noSender,         // For an outbound message, send ACK/NAK when this message has been transmitted
  sentAt:Long       = 0,                      // May be set by the application when created, if zero will be set
                                              // when passed to msgSend
  var xmitOrReceived:Long     = 0,            // Set when message is physically sent (if outbound) or received
  isPartOfFT:Boolean          = false,        // True == this is part of a File Transfer operation
  var appData:Option[AppData] = None,         // Optional application data - will be sent in all ACKs
  dataToString:(Array[Byte])=>String = (d:Array[Byte]) => ""  // Optional function which returns a String form of the data array
                                                              // used in the toString of the TMessage
)  extends JsonSupport
{
  import Transport._
  /** Unique key within THIS JVM - transient, should not be persisted */
  def transThisKey = if(trans.transID <= maskShort)
                       (trans.transID.toLong << (32 + 16)) | msgKey
                     else
                       throw new IllegalStateException(s"trans.transID must be 16-bit unsigned, had ${trans.transID}")

  /** transKey from the ORIGINATOR of this TMessage (will == transThisKey if we are originator) */
  def transOrigKey = if(transID==0)
                       transThisKey
                     else if(transID <= maskShort)
                       (transID.toLong << (32 + 16)) | msgKey
                     else
                       throw new IllegalStateException(s"transID must be 16-bit unsigned, had ${transID}")

  /** Unique key within a given transport - transient, should not be persisted */
  def msgKey       = if(connID <= maskShort)
                       (connID.toLong << 32) | msgID
                     else
                       throw new IllegalStateException(s"ConnID must be 16-bit unsigned, had $connID")

  override def toString = f"TMessage[Length: $length%,d, ConnID: $connID%,d, MsgID: $msgID%,d, Name: $name, appID: $appMsgID%,d, Flags: ${FError.flagNames(flags).mkString(",")}, Data: ${data.length}%,d bytes, DataStr: ${dataToString(data)}]"
  def strChunked   = if(isChunked) s" Chunked: T, DeChunk: ${if(deChunk) "T" else "F"}, First: $isFirstChunk, Last: $isLastChunk," else ""
  def strShort     = toString   // may want this to be shorter

  def isToXchange  = (flags & FToXchange.flag) > 0                // TMessage is directed to an Xchange server
  def isError      = (flags & FError.flag) > 0
  def isChunked    = (flags & FChunked.flag) > 0
  def isDeChunk    = (flags & FDeChunk.flag) > 0
  def isFirstChunk = (flags & FFirstChunk.flag) > 0
  def isLastChunk  = (flags & FLastChunk.flag) > 0
  def deChunk      = isChunked && (flags & FDeChunk.flag) > 0
  def szName       = if(name.isEmpty) 0 else name.getBytes.length
  def length       = szBaseInfo + szName + data.length
  def dataAsString = new String(data)

  def getError     = if(isError) new String(data).parseJson.convertTo[Error] else Error()

  def validChunk   = ((flags & FAnyChunk.flag) == 0) || isChunked     // Either all chunk flags OFF or isChunked is on (+ others maybe)

  def isPopulated  = connID != 0  && msgID != 0
  def isValid      = isPopulated && validChunk && name.getBytes.length <= maxSzName

  def isInbound    = (flags & FInbound.flag) > 0
  def isOutbound   = !isInbound

  def mustBeChunked = length > maxMessageSize       // Transport system must chunk/de-chunk this message

  /** Convert this expanded information into Array[Byte] buffer form, ready to transmit */
  def asBuffer(trans:Transport) = {
    if(mustBeChunked)         // Either this transport system or the application should have already chunked it
      throw new IllegalStateException(f"Message Conn: $connID%,d, MsgID: $msgID%,d, Name: $name -- must be chunked, Length: $length%,d > $maxMessageSize%,d")
    if(!isValid)
      throw new IllegalStateException(s"Message is not valid -- $strShort")
    val array = new Array[Byte](length)
    val bb    = ByteBuffer.wrap(array)

    bb.putInt(length)
    bb.putShort(((if(transID==0) trans.transID else transID) & maskShort).toShort)
    bb.putShort((connID & maskShort).toShort)
    bb.putInt( ((name.length<<szNameShift) + flags ).toInt )
    bb.putInt((msgID & maskInt).toInt)
    bb.putInt((appMsgID & maskInt).toInt)
    if(szName > 0) bb.put(name.getBytes)
    if(data.length > 0) bb.put(data)
    array
  }
}

/** Wrap a bytebuffer with helper methods to parse the buffer and return a TMessage.
 *  ByteBuffer MUST be
 *  -- BIG_ENDIAN
 *  -- have a backing array the EXACT SIZE of the message
 *  -- backing array must have a zero offset
 *
 *  @param bb     - The ByteBuffer
 *  @param trans  - must be relative to a Transport
 *
 */
case class OnTheWireBuffer( bb      :ByteBuffer,
                            trans   :Transport,
                            inbound :Boolean = true     // Usual case, but can override if needed
                          )
            extends JsonSupport {
  import Transport._
  // The val's must be extracted since in an Xchange server this is all that is needed to forward
  // a message to the output transport. All the rest are def's so only executed if needed
  val length       = bb.getInt(offsetLength)
  val transID      = bb.getShort(offsetTransID).toInt & maskShort     // NOTE: TransID of original creator of this TMessage
  val connID       = bb.getShort(offsetConnID).toInt & maskShort
  val nameAndFlags = bb.getInt(offsetSzNFlags).toLong & maskInt

  val msgID        = bb.getInt(offsetMsgID).toLong & maskInt
  var flags        = { val rslt = (nameAndFlags & maskFlags) | (if (inbound) FInbound.flag else 0)
                       if(bTransportOTW) debug(s"OTW - Length: $length, TransID: $transID, ConnID: $connID, MsgID: $msgID, Flags: ${FError.flagNamesStr(rslt)}")
                       rslt
                     }

  /** Unique key within a JVM - transient, should not be persisted */
  def transKey     = if(trans.transID <= maskShort)
                       (trans.transID << (32 + 16)) | msgKey
                     else
                       throw new IllegalStateException(s"TransID must be 16-bit unsigned, found ${trans.transID}")

  /** Unique key within a given transport - transient, should not be persisted */
  def msgKey       =  if(connID <= maskShort)
                        (connID << 32) | msgID
                      else
                        throw new IllegalStateException(s"ConnID must be 16-bit unsigned, found $connID")

  def array        = bb.array
  def nameSize     = (( nameAndFlags >> szNameShift) & maskSzName).toInt
  def appMsgID     = bb.getInt(offsetAppID).toLong & maskInt
  def name         = if(nameSize==0) "" else new String( bb.array, offsetName, nameSize )
  def posData      = offsetData(nameSize)
  def dataLength   = (length - posData)
  def isValidLength= length >= szBaseInfo && length <= maxMessageSize       // Length is not obviously screwy
  def isValidBfr   = bb.hasArray && bb.order==ByteOrder.BIG_ENDIAN && bb.limit() >= Transport.szBaseInfo

  def isError      = (flags & FError.flag) > 0

  def getError     = if(isError) new String(data).parseJson.convertTo[Error] else Error()

  def isValid      = isValidLength && isValidBfr

  def isInbound    = (flags & FInbound.flag) > 0
  def isOutbound   = !isInbound
  // These 'def's only exist if the data is chunked so compute only if needed
  def isChunked    = (flags & FChunked.flag) > 0
  def notChunked   = !isChunked
  def isFirstChunk = (flags & FFirstChunk.flag) > 0
  def isLastChunk  = (flags & FLastChunk.flag) > 0
  def isDeChunk    = (flags & FDeChunk.flag) > 0

  /** Return the data as an Array[Byte] - Note: dataLength may be 0 */
  def data         = if(dataLength > 0) Arrays.copyOfRange(bb.array, posData, posData + dataLength) else new Array[Byte](0)

  def fail         =if(!isValidLength) NAK.badLength
                    else if(!isValidBfr) NAK.badBuffer
                    else 0
  def baseToString  = f"Length: $length%,d, ConnID: $connID%,d, MsgID: $msgID%,d, AppMsgID: $appMsgID%,d, Flags: 0x$flags%X, SzName: $nameSize%d"

  /** Set one or more of the Flags on/off */
  def setFlags(to:Boolean, flagValue:Int*) = {
    flagValue.foreach( f => if(to) flags |= f else flags &= ~f)
  }
  /** Set all of the 'chunk' files OFF */
  def setNoChunks = flags &= ~FAnyChunk.flag

  /** Expand this buffer & return Message instance */
  def asMessage   = TMessage(trans = trans, transID = transID, flags = flags, connID = connID, msgID = msgID, appMsgID = appMsgID, name = name, data = data)
}

/******************************************************************************************************************/
/* Symbolic definition of the on-the-wire format. Actual processing is at the ByteBuffer level                    */
/* and all of the numerics except 'length' are UNSIGNED values. See TMessage & OnTheWireBuffer                    */
/******************************************************************************************************************/

// NOTE: 'length' does not need to be read as unsigned since it must be <= maxMessageSize
//       If Byte, Short, Int sign bit is ON, ByteBuffer stores as negative, but retrieved as unsigned by masking

// Note that frequently if 'chunked' the 'name' will appear only in the first chunk - to identify to the recipient what type of
// message this is - and then is szName == 0 afterwards. This is an application decision.
case class OnTheWireModel (                 // sz
  length:Int      = 0,                      //  4  - Length of this message/chunk, including this field
  transID:Int     = 0,                      //  2  - unique transID when TMessage first created, recovered in a received TMessage
  connID:Int      = 0,                      //  2  - Unique connection ID to allow multiplexing of logical connections in one Transport
  szName_flags:Int= 0,                      //  4  - first section contains the szName, second the Flags
  msgID:Int       = 0,                      //  4  - Unique Message ID Per Connection ID and unique only in EACH DIRECTION (inbound or outbound)
                                            //       since the SENDER assigns the Message ID. Note: Usually monotonic but no guarantee
                                            //       Will be the same value if there are multiple 'chunks' for this message.
  appMsgID:Int    = 0,                      //  4  - An Application-level message ID. Optional field, and if present not interpreted at all
                                            //       by the transport layer. Allows the application level logic to assign IDs that it can
                                            //       interpret.
  name:Array[Byte]= new Array[Byte](0),     //  n  - KEY: Defines the type of message. Could be a class name (e.g. sdm.RespGeneric or sdm.UserLogin)
                                            //            The sender & receiver must agree on all names and decide the format of the 'data' for this type of message.
                                            //            Automatic chunking sends the name only in the 1st chunk, szName == 0 in all following chunks
  data:Array[Byte]= new Array[Byte](0)      // (length - size of other fields)
){
  throw new IllegalStateException("OnTheWire is for descriptive purposes ONLY, it should never be instantiated. Use OnTheWireBuffer or TMessage")
}
/******************************************************/
/** Definition of all the TMessage flags              */
sealed trait TFlag {
  def name:String
  def flag:Int

  val allFlags = List(FError, FInbound, FToXchange, FFileTransfer, FChunked, FDeChunk, FFirstChunk, FLastChunk, FAnyChunk )

  def flagNames(flags:Long) = allFlags.filter( f => Transport.flagsAny(flags, f.flag) ).map( _.name )
  def flagNamesStr(flags:Long) = flagNames(flags).mkString(",")
}
case object FError        extends TFlag { val name = "FError";        val flag = 0x01<<1 }     // This is an ERROR report, the 'data' will contain an Error class in JSON
case object FInbound      extends TFlag { val name = "FInbound";      val flag = 0x01<<2 }     // This is an INBOUND message (i.e. it was read from the Channel)
case object FToXchange    extends TFlag { val name = "FToXchange";    val flag = 0x01<<3 }     // This message is directed to the Xchange server, not the Server/Viewer
case object FFileTransfer extends TFlag { val name = "FFileTransfer"; val flag = 0x01<<4 }     // Message is part of a file transfer operation. Applications will usually
                                                                                                // ignore these.
case object FChunked      extends TFlag { val name = "FChunked";      val flag = 0x01<<5 }     // This message has been chunked (may still be only 1 chunk)
case object FDeChunk      extends TFlag { val name = "FDeChunk";      val flag = 0x01<<6 }     // Receiver should de-chunk this message before sending it to the application
case object FFirstChunk   extends TFlag { val name = "FFirstChunk";   val flag = 0x01<<9 }     // This is the first chunk
case object FLastChunk    extends TFlag { val name = "FLastChunk";    val flag = 0x01<<8 }     // This is the last chunk
case object FAnyChunk     extends TFlag { val name = "FAnyChunk";     val flag = FChunked.flag | FDeChunk.flag | FFirstChunk.flag | FLastChunk.flag }
// WARNING: Update the 'allFlags' list above if adding/dropping a flag
