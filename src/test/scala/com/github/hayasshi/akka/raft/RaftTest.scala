package com.github.hayasshi.akka.raft

import org.scalatest.diagrams.Diagrams
import org.scalatest.freespec.AnyFreeSpec
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import org.scalatest.freespec.AnyFreeSpecLike
import akka.testkit.TestKit
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.testkit.ImplicitSender

import com.github.hayasshi.akka.raft.{RaftNode, Follower}
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.util.Timeout
import scala.concurrent.ExecutionContext

class RaftTest extends TestKit(ActorSystem("RaftTest")) with ImplicitSender with AnyFreeSpecLike with Diagrams {

  implicit val timeout: Timeout = 5.seconds
  implicit val ec: ExecutionContext = system.dispatcher

  "Raft" - {

    class Target extends Actor with ActorLogging with RaftNode with Follower with Candidate with Leader

    def askMessage(to: ActorRef, msg: RaftProtocol.Command): Unit = {
      (to ? msg).foreach {
        case res: RaftProtocol.Accepted =>
          system.log.info(s"Test receive $res")
        case Follower.RedirectTo(Some(leader), cmd) =>
          askMessage(leader, cmd)
        case Follower.RedirectTo(None, _) =>
          system.log.info("No leader yet.")
      }
    }

    def stopLeader(initial: ActorRef): Unit = {
      (initial ? RaftProtocol.Command(0)).foreach {
        case _: RaftProtocol.Accepted =>
          system.log.info(s"Stop leader: $initial")
          system.stop(initial)
        case Follower.RedirectTo(Some(leader), _) =>
          system.log.info(s"Stop leader: $leader")
          system.stop(leader)
        case Follower.RedirectTo(None, _) =>
          system.log.info("No leader yet.")
      }
    }
      
    "Test1" in {
      val member1 = system.actorOf(Props[Target](new Target), "member1")
      val member2 = system.actorOf(Props[Target](new Target), "member2")
      val member3 = system.actorOf(Props[Target](new Target), "member3")
      val member4 = system.actorOf(Props[Target](new Target), "member4")
      val member5 = system.actorOf(Props[Target](new Target), "member5")

      member1 ! RaftNode.Start(Set(member2, member3, member4, member5))
      member2 ! RaftNode.Start(Set(member1, member3, member4, member5))
      member3 ! RaftNode.Start(Set(member1, member2, member4, member5))
      member4 ! RaftNode.Start(Set(member1, member2, member3, member5))
      member5 ! RaftNode.Start(Set(member1, member2, member3, member4))

      Thread.sleep(10 * 1000)

      askMessage(member3, RaftProtocol.Command(2))

      Thread.sleep(2 * 1000)

      stopLeader(member1)

      Thread.sleep(5 * 1000)

      askMessage(member2, RaftProtocol.Command(3))

      Thread.sleep(3 * 1000)

      Await.result(system.terminate(), 10.seconds)
    }

  }
  
}
