package com.github.hayasshi.akka.raft

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import com.github.hayasshi.akka.raft.ReceiverActor.ReceiverHandlerActor

import java.net.InetSocketAddress

class ReceiverActor(bindHost: String, bindPort: Int)
    extends Actor
    with ActorLogging {

  val tcp: ActorRef = IO(Tcp)(context.system)
  tcp ! Bind(self, new InetSocketAddress(bindHost, bindPort))

  var connections = Map.empty[ActorRef, InetSocketAddress]

  override def receive: Receive = {
    case b @ Bound(_) =>
      log.info(s"Bind to $b")

    case CommandFailed(b: Bind) =>
      log.error(s"Failed to bind: $b")
      context.stop(self)

    case Connected(remote, _) =>
      log.info(s"Connected from $remote")
      val remoteRef = sender()
      val handler =
        context.actorOf(Props(new ReceiverHandlerActor(remoteRef, remote)))
      context.watch(handler)
      remoteRef ! Register(handler)
      connections += (handler, remote)

    case Terminated(ref) =>
      connections.removed(ref)
  }
}

object ReceiverActor {

  class ReceiverHandlerActor(
      remoteRef: ActorRef,
      remoteAddress: InetSocketAddress
  ) extends Actor
      with ActorLogging {

    log.info(s"Created handler with remoteRef: $remoteRef")

    override def receive: Receive = {
      case Received(data) =>
        val fromRef = sender()
        log.info(s"Received message from $fromRef: $data")
        remoteRef ! Write(data)

      case PeerClosed =>
        log.info("Connection closed")
        context.stop(self)
    }
  }

}
