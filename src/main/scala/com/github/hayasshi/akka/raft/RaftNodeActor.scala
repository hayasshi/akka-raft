package com.github.hayasshi.akka.raft

import akka.actor.{Actor, ActorLogging, ActorRef}

class State[A]

case class RaftSettings()

class RaftNodeActor(
    seedMembers: Set[ActorRef],
    raftSettings: RaftSettings
) extends Actor
    with ActorLogging {

  override def receive: Receive = follower // initial state

  def candidate: Receive = ???
  def leader: Receive = ???
  def follower: Receive = ???
}
