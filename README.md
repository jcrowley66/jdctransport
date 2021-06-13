# Messaging
A basic low-level messaging library based on AKKA Actors.

This is intended to:
- read data from an InputStream
- send data to an OutputStream
- define a standard low-level buffer format for in/out streams 
- optionally convert the input/output data to SSL
- convert input buffers to a high-level case class (**Message**)
- convert a high-level Message instance to an output buffer
- forward all inbound messages to an **Application**
- accept Messages from the Application and send to the OutputStream

A **MessagingActor** is the control point for a single Messaging system. It
will be given the InputStream and OutputStream to process, and these should
be exclusive to this MessagingActor. It is also given a **msgIDGenerator** so
that all messages can be assigned a unique ID.

The MessagingActor in turn will start a **MessagingInActor** and a **MessagingOutActor**.

Important concepts:
- every Connection is assigned a unique ConnectionID
- each ConnectionID is associated with an **ApplicationActor** (created by the
  caller and sent to the MessagingActor at startup)
- every Message is assigned a unique MessageID 
- one Messaging instantiation may multiplex several ConnectionIDs
    - One **Startup** and one or more **StartConn** messages will be sent to a MessagingActor
    - The same ApplicationActor may be used for each Connection, a different
    ApplicationActor for each Connection, or any combination. This is all 
      determined by the caller who intially created the MessagingActor.
      
# Xchange Server
Allows **Messaging** instances on Servers and Clients to register and cross-connect
so that all messages are transparently forwarded.

If both a Server and a Client are **visible** to each other, then they can connect directly.
But if either is not visible, e.g. behind a firewall or on an internal network, then both
can connect to a visible Xchange server, register, and cross-connect.

Once cross-connected, the Xchange server will forward all messages sent by one party to the other party.

The Connection IDs assigned by the Xchange server when each party first connects are used to match 
the parties, and these may be multiplexed. So, for example, a Server may connect to an Xchange server
and then accept connections from several clients. The traffic to/from all of the clients may then be
multiplexed over a single physical connection to the Server.
      


