package com.github.hayasshi.akka.raft

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.io.Tcp.{
  Close,
  CommandFailed,
  Connect,
  Connected,
  ConnectionClosed,
  Received,
  Register,
  Write
}
import akka.io.{IO, Tcp}
import akka.util.ByteString

import java.net.InetSocketAddress

class SenderActor extends Actor with ActorLogging {
  import SenderActor._

  val tcp: ActorRef = IO(Tcp)(context.system)

  val connections = collection.mutable.Map.empty[InetSocketAddress, ActorRef]

  override def receive: Receive = {
    case AddRemote(remote) =>
      tcp ! Connect(remote)

    case CommandFailed(c: Connect) =>
      log.error(s"Failed to connect: $c")
    // TODO: Notice to parent

    case Connected(remote, _) =>
      log.info(s"Connected to $remote")
      val connection = sender()
      val handler = context.actorOf(Props(new Actor with ActorLogging {
        override def receive: Receive = {
          case data: ByteString =>
            connection ! Write(data)
          case CommandFailed(w: Write) =>
            // O/S buffer was full
            // TODO impl
            log.error(s"Failed to write data: $w")
          case Received(data) =>
          // TODO impl
          case "close" =>
            // TODO impl
            connection ! Close
          case _: ConnectionClosed =>
            // TODO impl
            context.stop(self)
        }
      }))
      connection ! Register(handler)
      connections.update(remote, handler)
  }
}

object SenderActor {
  case class AddRemote(remote: InetSocketAddress)
}
