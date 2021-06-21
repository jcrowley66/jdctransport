package jdctransport

import java.util.Arrays
import akka.actor.{Actor, ActorRef}
import java.nio.{ByteOrder, ByteBuffer}

// Optional data which exists only within the sender - it does not go over-the-wire -- But will be included in ACK & NACK messages
// Whether this data is populated and/or used is completely under the control of the application
// SUGGESTED data & meaning
case class AppData( fullMessage:Option[Message]=None,     // Recursive link to the complete Message
                    // If data is chunked, these can be useful. Especially useful in ACKs so that
                    // other Senders can determine if sending should be throttled until the queue is reduced
                    totalData:Long=0,                     // Total data
                    sentSoFar:Long=0,                     // Total data sent so far
                    numChunks:Int=0,                      // Number of chunks
                    numThisChunk:Int=0,                   // Number of this chunk - 1-based

                    otherData:Option[Any] = None          // Catch all if Application needs additional information
                  )
// An original or expanded Message -- all of the numeric fields are larger to hold the unsigned number from the ByteBuffer
// The 'asBuffer' method converts to an Array[Byte] form for transmission. See OnTheWireBuffer to parse an inbound buffer and
// produce a Message.
case class Message (
  // 'length', 'marker', and 'szName' are computed below
  flags    :Int     = 0,                      // Bit flags for certain situations - see Transport.flag... values
  connID   :Long    = 0,                      // ID of the logical Connection to which this message belongs
  msgID    :Long    = 0,                      // ID of this message. Note: For chunked messages, ID will be the same for all chunks
  name     :String  = "",                     // KEY: Defines the type of message. Could be a class name (e.g. mypackage.MyClass)
                                              //      The sender & receiver must decide on all of the 'name' values and the format of the 'data'
                                              //      for a message of this name. E.g. based on the 'name', the data might be a String,
                                              //      JSON, Java object serialization, etc.
                                              //      NOTE: String.length may NOT be the same as the Array[Byte] length if any non-Ascii
                                              //            characters appear in the name
  data:Array[Byte]  = new Array[Byte](0),     // The actual data. For normal messages data.length == length - sizeBaseInfo - name.getBytes.length

  var appData:Option[AppData] = None          // Optional application data - will be sent in all ACKs to ALL registered senders
) {
  import Transport._
  def isChunked    = (flags & Transport.flagChunked) > 0
  def isDeChunk    = (flags & Transport.flagDeChunk) > 0
  def isFirstChunk = (flags & Transport.flagFirstChunk) > 0
  def isLastChunk  = (flags & Transport.flagLastChunk) > 0
  def deChunk      = isChunked && (flags & Transport.flagDeChunk) > 0
  def szName       = if(name.isEmpty) 0 else name.getBytes.length
  def length       = szBaseInfo + szName + data.length
  def dataAsString = new String(data)

  def validChunk   = ((flags & anyChunkFlag) == 0) || isChunked     // Either all chunk flags OFF or isChunked is on (+ others maybe)

  def isPopulated  = connID != 0  && msgID != 0 && (name.nonEmpty || isChunked)
  def isValid      = isPopulated && validChunk

  def isInbound    = (flags & Transport.flagInbound) > 0
  def isOutbound   = !isInbound

  def strChunked = if(isChunked) s" Chunked: T, DeChunk: ${if(deChunk) "T" else "F"}, First: $isFirstChunk, Last: $isLastChunk," else ""
  def strShort   = f"Message[Lnth: $length%,d,$strChunked ConnID: $connID%,d, MsgID: $msgID%,d, DataSz: ${data.length}%,d, Name: $name]"

  def mustBeChunked = length > maxMessageSize       // Transport system must chunk/de-chunk this message

  /** Convert this expanded information into Array[Byte] buffer form, ready to transmit */
  def asBuffer     = {
    if(mustBeChunked)         // Either this transport system or the application should have already chunked it
      throw new IllegalStateException(f"Message Conn: $connID%,d, MsgID: $msgID%,d, Name: $name -- must be chunked, Length: $length%,d > $maxMessageSize%,d")
    if(!isPopulated)
      throw new IllegalStateException(s"Message missing key data -- $strShort")
    val array = new Array[Byte](length)
    val bb    = ByteBuffer.wrap(array)
    bb.putInt(length)
    bb.putInt(0)            // Hash == 0, reset after computing the actual hash
    bb.put(flags.toByte)
    bb.put(szName.toByte)
    bb.putInt(connID.toInt)
    bb.putInt(msgID.toInt)

    if(szName > 0) bb.put(name.getBytes)
    bb.put(data)

    val hash = Arrays.hashCode(array)         // Compute the hash with hash field == 0, then stuff into buffer
    bb.putInt(offsetHash, hash)
    array
  }
}

/** Wrap a bytebuffer with helper methods.
 *  ByteBuffer MUST be
 *  -- BIG_ENDIAN
 *  -- have a backing array
 *  -- backing array must have a zero offset
 *
 *  @param isInbound - sets as inbound/outbound - defaults to Inbound since this class is parsing a low-lever buffer
 *                     Note: Sender considers this as an Outbound message, Receiver as Inbound
 */
case class OnTheWireBuffer( bb:ByteBuffer, isInboundParam:Boolean = true ){
  import Transport._
  // All of the 'val's are fields or logic which must always exist in the message
  val array        = bb.array
  val length       = bb.getInt(offsetLength)
  val hash         = bb.getInt(offsetHash).toLong & maskInt
  val flags:Int    = (((bb.get(offsetFlags) & ~Transport.flagInbound) | (if(isInboundParam) Transport.flagInbound else 0)).toInt & maskByte)
  val nameSize     = bb.get(offsetSzName).toShort & maskByte
  val connID       = bb.getInt(offsetConnID).toLong & maskInt
  val msgID        = bb.getInt(offsetMsgID).toLong & maskInt
  val name         = if(nameSize==0) "" else new String( bb.array, offsetName, nameSize )
  val posData      = szBaseInfo + nameSize
  val dataLength   = (length - posData)
  val isValidLength= length >= szBaseInfo && length <= maxMessageSize       // Length is not obviously screwy
  val isValidBfr   = bb.hasArray && bb.order==ByteOrder.BIG_ENDIAN && bb.limit() >= Transport.szBaseInfo
  val isValidHash  = {  // Set the in-buffer hash field to 0, compute the hash, put the original hash back
                        bb.putInt(offsetHash, 0)
                        val hashW0 = Arrays.hashCode(array)
                        bb.putInt(offsetHash, hash.toInt)
                        hashW0 == hash
                      }
  def isValid      = isValidLength && isValidBfr && isValidHash

  def isInbound    = (flags & Transport.flagInbound) > 0
  def isOutbound   = !isInbound
  // These 'def's only exist if the data is chunked so compute only if needed
  def isChunked    = (flags & Transport.flagChunked) > 0
  def notChunked   = !isChunked
  def isFirstChunk = (flags & Transport.flagFirstChunk) > 0
  def isLastChunk  = (flags & Transport.flagLastChunk) > 0
  def isDeChunk    = (flags & Transport.flagDeChunk) > 0

  /** Return the data as an Array[Byte] - Note: dataLength may be 0 */
  def data         = if(dataLength > 0) Arrays.copyOfRange(bb.array, posData, posData + dataLength) else new Array[Byte](0)

  def fail         =if(!isValidLength) NACK.badLength
                    else if(!isValidBfr) NACK.badBuffer
                    else if(!isValidHash) NACK.hashFailed
                    else 0
  def baseToString  = f"Length: $length%,d, ConnID: $connID%,d, MsgID: $msgID%,d, Flags: 0x$flags%X, SzName: $nameSize%d"
  /** Expand this buffer & return Message instance */
  def asMessage   = Message(flags = flags, connID = connID, msgID = msgID, name = name, data = data)
}

/******************************************************************************************************************/
/* Symbolic definition of the on-the-wire format. Actual processing is at the ByteBuffer level                    */
/* and all of the numerics except 'length' & 'totalData' are UNSIGNED values. See Message & OnTheWireBuffer       */
/******************************************************************************************************************/

// NOTE: 'length' does not need to be read as unsigned since it must be <= maxMessageSize
//       'totalData' also is not read as unsigned since it is a Long and inherently large enough
//       If Byte, Short, Int sign bit is ON, ByteBuffer stores as negative, but retrieved as unsigned by masking

// Note that frequently if 'chunked' the 'name' will appear only in the first chunk - to identify to the recipient what type of
// message this is - and then is szName == 0 afterwards. This is an application decision.
// Also, the data in the first chunk may differ from later chunks. For example, for a file transfer may include both
// the name - e.g. FileTransfer - and in 'data' have JSON with the full target path of the file, creation time, etc.
// Then all subsequent chunks have no name, and the 'data' is just bytes to write into the target file.
// The connID/msgID combination will be the mechanism for unique identification of all the related chunks.
case class OnTheWireModel (                      // sz
  length:Int      = 0,                      //  4  - Length of this message/chunk, including this field
  totalData:Long  = 0,                      //  8 -  Total logical size of 'data' - same as data.length in normal messages, larger if chunked
  hash:Int        = 0,                      //  4  - hash of this message buffer computed WITH this field itself == 0
  flags:Byte      = 0,                      //  1  - Bit flags for certain situations - see Messaging.flag... values
  szName:Byte     = 0,                      //  1  - Size of the 'name' array -- 0 == no name
  connID:Int      = 0,                      //  4  - Unique connection ID to allow multiplexing of logical connections
  msgID:Int       = 0,                      //  4  - Unique Message ID Per Connection ID and unique only in EACH DIRECTION (inbound or outbound)
                                            //       since the SENDER assigns the Message ID. Note: Usually monotonic but no guarantee
                                            //       Will be the same value if there are multiple 'chunks' for this message.
  name:Array[Byte]= new Array[Byte](0),     //  n  - KEY: Defines the type of message. Could be a class name (e.g. sdm.RespGeneric or sdm.UserLogin)
                                            //            The sender & receiver must agree on all names and decide the format of the 'data' for this type of message.
                                            //            Automatic chunking sends the name only in the 1st chunk, szName == 0 in all following chunks
  data:Array[Byte]= new Array[Byte](0)      // (length - size of other fields)
){
  throw new IllegalStateException("OnTheWire is for descriptive purposes ONLY, it should never be instantiated. Use OnTheWireBuffer or Message")
}
