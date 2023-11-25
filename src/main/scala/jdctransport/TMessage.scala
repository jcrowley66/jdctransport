package jdctransport

import java.util.Arrays
import akka.actor.{ActorRef, Actor}

import java.nio.{ByteOrder, Buffer, ByteBuffer}     // WARNING: When using ByteBuffer see bbPosition(...) in Transport
import spray.json._
import jdctransport.Transport.{szBaseInfo, flagOn, flagsOn, keyHighBits}
import jdccommon._

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

/** Common trait supported by both TMessage and the OTWMessage
 *  Normally, outbound messages are created in TMessage format and inbound messages arrive in OTWMessage (buffer) format
 *  Both message types support the asTMessage, asOTW, and asBuffer methods to convert from one form to another.
 **/
sealed trait TransMessage extends CallbackTarget with JsonSupport {
  // These are defined as val's in both TMessage and OTW
  def trans:Transport
  def transID:Int
  def connID:Int
  def msgID:Long
  // End of the val's

  /** Unique key within THIS JVM - transient, should not be persisted */
  def transThisKey =  Transport.makeKey(trans.transID, msgID)
  /** transKey from the ORIGINATOR of this TMessage (will == transThisKey if we are originator) */
  def transOrigKey =  if(transID==0) transThisKey else Transport.makeKey(transID, msgID)
  /** Unique key for message within a given transport - transient, should not be persisted */
  def msgKey       =  Transport.makeKey(connID, msgID)
  /** Implement the methods from CallbackTarget */
  def cbID         = transThisKey
  override
  def cbKeyHighBits= keyHighBits

  //////////// These are implemented differently by TMessage vs OTW
  def length:Int
  def szName:Int
  def name:String
  def isValid:Boolean
  def data:Array[Byte]
  def appMsgID:Long
  def flags:Long              // The current flag settings
  // Modify flags in-place. Returns TRUE if any change, FALSE if those flags already set or cleared
  def setFlags(these:TFlag*):Boolean    = setFlags(TFInbound.flagValues(these))
  def setFlags(these:Long):Boolean
  def clearFlags(these:TFlag*):Boolean  = clearFlags(TFInbound.flagValues(these))
  def clearFlags(these:Long):Boolean
  /////////////////////////////////////////////////////////////////
  def isToTransport= (flags & TToTransport.flag) > 0              // TMessage is directed to Transport itself
  def isError      = (flags & TFError.flag) > 0
  def isChunked    = (flags & TFChunked.flag) > 0
  def notChunked   = !isChunked
  def isDeChunk    = (flags & TFDeChunk.flag) > 0 && trans.app.transSkipDeChunk==false
  def isAppDeChunk = (flags & TFAppDeChunk.flag) > 0
  def mustDeChunk  = isChunked && ( isDeChunk || isToTransport || isAppDeChunk)
  def isFirstChunk = (flags & TFFirstChunk.flag) > 0
  def isLastChunk  = (flags & TFLastChunk.flag) > 0
  def strChunked   = if(isChunked) s" Chunked: T, DeChunk: ${if(mustDeChunk) "T" else "F"}, First: $isFirstChunk, Last: $isLastChunk," else ""
  def validChunk   = ((flags & TFAnyChunk.flag) == 0) || isChunked     // Either all chunk flags OFF or isChunked is on (+ others maybe)
  def isPopulated  = connID != 0  && msgID != 0
  def isInbound    = (flags & TFInbound.flag) > 0
  def isOutbound   = !isInbound
  def mustBeChunked= length > Transport.maxMessageSize            // Transport system must chunk/de-chunk this message
  def isValidLength= length >= szBaseInfo                         // Length is not obviously screwy

  def isConnCreated= (flags & TFConnCreated.flag) > 0             // Special msg to Viewer or Publisher to assign the ConnID
  def isFTStart    = (flags & TFTStart.flag) > 0                  // FileTransfer Start msg sent to other end of connection
  def isFTReady    = (flags & TFTReady.flag) > 0                  // FileTransfer Ready - other end is ready to start
  def isFTStop     = (flags & TFTStop.flag) > 0                   // Stop a File Transfer
  def isFTRefused  = (flags & TFTRefused.flag) > 0                // Other end refused a FileTransfer Start request
  def isFTSegment  = (flags & TFTSegment.flag) > 0                // Marks all FileTransfer segments. App usually ignores
  def isFTFileTimes= (flags & TFTFileTimes.flag) > 0              // Outbound side is reporting file times to inbound (if to be preserved)

  def isFTAny      = isFTStart || isFTReady || isFTStop || isFTRefused || isFTSegment || isFTFileTimes

  def getError     = if(isError) new String(data).parseJson.convertTo[Error] else Error()

  def flagNames    = { TFError.presentNamesStr(flags) }

  def reset():TransMessage            // Reset this message to initial conditions

  def dup:TransMessage                // DUPLICATE the current message. Needed, e.g., if planning to send the message
                                      // to more than one place and some data, e.g. flags or the position (if OTW) may change
                                      // If an OTW message, position set to 0, limit & capacity set to 'length' in dup message
                                      // NOTE: use asTMessage, asOTW, etc to get the final form desired
                                      // Did not use copy() to avoid clash with case class copy(...) for TMessage
  def asTMessage:TMessage             // Convert this message to other formats
  def asOTW:OTWMessage
  def asBuffer:Array[Byte]

  def toShort      = {var label = this.getClass.getSimpleName
                      if(label=="OTWMessage") label = "OTW"
                      f"$label[Length: $length%,4d, ConnID: $connID%,3d, MsgID: $msgID%,4d, Name: $name, appID: $appMsgID%,d, Flags: $flags $flagNames, Data: ${data.length}%,d bytes]"
                    }
  def toVeryShort  = f"OTW[${Transport.parseKeyStr(msgID)}, Name: $name, Lnth: $length%,d]"
  def strShort     = toShort

  override
  def toString     = toShort.dropRight(1) + s", Data: ${new String(data)}"
}
/** An original or expanded Message -- all of the numeric fields are larger to hold the unsigned number from the ByteBuffer
 *  The 'asBuffer' method converts to an Array[Byte] form for transmission. See OTWMessage to parse an inbound buffer and
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
  var flags:Long    = 0,                      // Bit flags for certain situations - see Transport.flag... values MAY BE CHANGED
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
)  extends TransMessage with JsonSupport
{
  import Transport._

  if(connID > Transport.maskShort) throw new IllegalStateException(s"ConnID must be 16-bit unsigned, had $connID")

  def szName       = if(name.isEmpty) 0 else name.getBytes.length
  def length       = szBaseInfo + szName + data.length
  def isValid      = isPopulated && validChunk && name.getBytes.length <= maxSzName

  def setFlags(these:Long) = if( (flags & these) != these){
                                flags |= these
                                true
                              } else
                                false
  def clearFlags(these:Long) = if( (flags & these) > 0){
                                  flags &= ~these
                                  true
                               } else
                                  false

  def reset()               = { /** No reset needed for a TMessage */
                                  this
                              }
  def dup:TransMessage      = this.copy()

  def asTMessage:TMessage   = this
  def asOTW:OTWMessage      = OTWMessage(ByteBuffer.wrap(asBuffer), trans)
  /** Convert this expanded information into Array[Byte] buffer form, ready to transmit */
  def asBuffer = {
    if(!isValid)
      throw new IllegalStateException(s"Message is not valid -- $strShort")
    val array = new Array[Byte](length)
    val bb    = ByteBuffer.wrap(array)          // WARNING: If use position(n) or limit(n) see fixup routines in Transport
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
 *  ByteBuffer MUST be BIG_ENDIAN
 *
 *  @param bb     - The ByteBuffer
 *  @param trans  - must be relative to a Transport
 *
 */
case class OTWMessage( bb   :ByteBuffer,
                       trans:Transport
                     )
            extends TransMessage with JsonSupport {
  import Transport._
  // The val's must be extracted since in an Xchange server this is all that is needed to forward
  // a message to the output transport. All the rest are def's so only executed if needed
  val length       = bb.getInt(offsetLength)
  val transID      = bb.getShort(offsetTransID).toInt & maskShort     // NOTE: TransID of original creator of this TMessage
  val connID       = bb.getShort(offsetConnID).toInt & maskShort
  val msgID        = bb.getInt(offsetMsgID).toLong & maskInt

  lazy val szName  = (( nameAndFlags >> szNameShift) & maskSzName).toInt
  lazy val name    = if(szName==0) "" else new String( bb.array, offsetName, szName )
  /** Return the data as an Array[Byte] - Note: dataLength may be 0 */
  lazy val data    = if(dataLength > 0) Arrays.copyOfRange(bb.array, posData, posData + dataLength) else new Array[Byte](0)

  def nameAndFlags = bb.getInt(offsetSzNFlags).toLong & maskInt       // 'def' since flags may be changed
  def flags        = nameAndFlags & maskFlags
  def array        = bb.array
  def appMsgID     = bb.getInt(offsetAppID).toLong & maskInt
  def posData      = offsetData(szName)
  def dataLength   = (length - posData)
  def isValidBfr   = bb.hasArray && bb.order==ByteOrder.BIG_ENDIAN && bb.asInstanceOf[Buffer].limit() >= Transport.szBaseInfo
  def isValid      = isValidLength && isValidBfr


  def fail         =if(!isValidLength) NAK.badLength
                    else if(!isValidBfr) NAK.badBuffer
                    else 0
  def baseToString  = f"Length: $length%,d, ConnID: $connID%,d, MsgID: $msgID%,d, AppMsgID: $appMsgID%,d, Flags: 0x$flags%X, SzName: $szName%d"

  def setFlags(these: Long): Boolean = if( (flags & these) != these) {
                                          bb.putInt(offsetSzNFlags, ((szName<<szNameShift) + ((flags.toInt | these.toInt) & maskFlags) ))
                                          true
                                       } else
                                          false

  def clearFlags(these: Long): Boolean = if( (flags & these) > 0){
                                            bb.putInt(offsetSzNFlags, ((szName<<szNameShift) + ((flags.toInt & ~(these.toInt)) & maskFlags) ))
                                            true
                                         } else
                                            false

  def reset()               = { // Reset the ByteBuffer
                                  bb.asInstanceOf[Buffer].position(0).limit(length)
                                  this
                              }
  def dup:TransMessage      = { // Do not want to chg position, limit, etc of existing ByteBuffer so ....
                                val newbytes = new Array[Byte](length)
                                val oldbytes = bb.array()
                                System.arraycopy(oldbytes, 0, newbytes, 0, length)
                                val newbb = ByteBuffer.wrap(newbytes)
                                newbb.asInstanceOf[Buffer].position(0).limit(length)
                                OTWMessage(newbb, trans)
                              }

  /** Expand this buffer & return TMessage instance */
  def asTMessage          = TMessage(trans = trans, transID = transID, flags = flags, connID = connID, msgID = msgID, appMsgID = appMsgID, name = name, data = data)
  def asOTW: OTWMessage   = this
  def asBuffer:Array[Byte]= bb.array

}

/******************************************************************************************************************/
/* Symbolic definition of the on-the-wire format. Actual processing is at the ByteBuffer level                    */
/* and all of the numerics except 'length' are UNSIGNED values. See TMessage & OTWMessage                         */
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
  throw new IllegalStateException("OnTheWire is for descriptive purposes ONLY, it should never be instantiated. Use OTWMessage or TMessage")
}
/******************************************************/
/** Definition of all the TMessage flags              */
/** NOTE: The calling application may add it's own    */
/**       flag values at the end. See JDCFlagsCodes.  */
/**                                                   */
/** JDCFlagsCodes is a late addition, so code could   */
/** be re-factored further to eliminate dup logic in  */
/** this trait, but did not want to chance breaking it*/
/******************************************************/
trait TFlag extends jdccommon.JDCFlagsCodes{
  def name: String
  def flag: Int

  val anyFileTransfer:List[TFlag] = List(TFTStart, TFTReady, TFTStop, TFTRefused, TFTSegment, TFTFileTimes)
  val anyChunk:List[TFlag]        = List(TFChunked, TFDeChunk, TFFirstChunk, TFLastChunk)       // NOTE: TFAppDeChunk not included
  // All base flags - exclude any combinations, e.g. FAnyChunk
  val allTFlags:List[TFlag]      = List(TFInbound, TFChunked, TFDeChunk, TFFirstChunk, TFLastChunk, TToTransport) ++
                                    anyFileTransfer ++
                                    List(TFConnCreated, TFAppDeChunk)

  def allActive                   = allTFlags
  val maxTFlag                    = allTFlags.map(_.flag).max

  override def allExtended              = allActive
  override def allExcluded:List[TFlag]  = List(TFError)

  def myValues:List[TFlag]        = allValues.map(_.asInstanceOf[TFlag])

  /////// Adapting logic to JDCFlagsCodes so we don't change names throughout /////
  def value:Long        = flag
  val description       = ""
  val bitFlags          = true
  ////// end of adaptors //////

  def flagValueCheckOK= allTFlags.forall( _.flag <= Transport.maxFlagValue )

  def flagValues(these:Seq[TFlag]):Long   = these.foldLeft(0)( (accum, tflag) => accum | tflag.flag)

  // Utility methods that assume only the flags defined within Transport
  def flagNames(flags:Long):Seq[String]         = flagNames(flags, allTFlags)
  def flagNamesStr(flags:Long):String           = flagNamesStr(flags, allTFlags)
  // These versions allow passing in the 'all' list, which may include additional flags defined at the application level
  def flagsAll(flags:TFlag*):Int                = flags.foldLeft(0)( (a, b) => a | b.flag)
  def flagNames(flags:Long, all:Seq[TFlag])     = all.filter( f => Transport.flagsAny(flags, f.flag) ).map( _.name )
  def flagNamesStr(flags:Long, all:Seq[TFlag])  = flagNames(flags, all).mkString(",")
}
case object TFError       extends TFlag { val name = "TFError";       val flag = 0x01<<0  }   // This is an ERROR report, the 'data' will contain an Error class in JSON
case object TFInbound     extends TFlag { val name = "TFInbound";     val flag = 0x01<<1  }   // This is an INBOUND message (i.e. it was read from the Channel)
case object TFChunked     extends TFlag { val name = "TFChunked";     val flag = 0x01<<2  }   // This message has been chunked (may still be only 1 chunk)
case object TFDeChunk     extends TFlag { val name = "TFDeChunk";     val flag = 0x01<<3  }   // Receiver should de-chunk this message before sending it to the application
case object TFFirstChunk  extends TFlag { val name = "TFFirstChunk";  val flag = 0x01<<4  }   // This is the first chunk
case object TFLastChunk   extends TFlag { val name = "TFLastChunk";   val flag = 0x01<<5  }   // This is the last chunk
case object TFAnyChunk    extends TFlag { val name = "TFAnyChunk";    lazy val flag = flagsAll(anyChunk: _*) } // 'lazy' or might init to ZERO!
    // Messages handled by Transport system itself, not passed to the application
case object TToTransport  extends TFlag { val name = "TFToTransport"; val flag = 0x01<<6  }   // General msg to the Transport itself, not the application &
                                                                                              // not one of the specific cases below. Will usually examine the 'name'
case object TFTStart      extends TFlag { val name = "TFTStart";      val flag = 0x01<<7  }
case object TFTReady      extends TFlag { val name = "TFTReady";      val flag = 0x01<<8  }
case object TFTStop       extends TFlag { val name = "TFTStop";       val flag = 0x01<<9  }
case object TFTRefused    extends TFlag { val name = "TFTRefused";    val flag = 0x01<<10 }
case object TFTSegment    extends TFlag { val name = "TFTSegment";    val flag = 0x01<<11 }   // Message is part of a file transfer operation. Applications will usually ignore these.
case object TFTFileTimes  extends TFlag { val name = "TFTFileTimes";  val flag = 0x01<<12 }   // Message with file times if outbound needs to send to inbound
case object TFConnCreated extends TFlag { val name = "TFConnCreated"; val flag = 0x01<<13 }   // Notify Application that a ConnID has been assigned
case object TFAppDeChunk  extends TFlag { val name = "TFAppDeChunk";  val flag = 0x01<<14 }   // Application wants to force a de-chunk
// WARNING: Update the 'allTFlags' list above if adding/dropping a flag
// NOTE: Highest 'flag' must be within the bitsInFlags value!!!
