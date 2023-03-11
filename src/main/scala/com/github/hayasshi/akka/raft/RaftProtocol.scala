package com.github.hayasshi.akka.raft

import akka.actor.ActorRef

object RaftProtocol {

  case class Log(
      term: Int,
      index: Int,
      value: Int
  )

  case class AppendEntries(
      term: Int,
      leader: ActorRef,
      prevLogIndex: Int,
      prevLogTerm: Int,
      entries: IndexedSeq[Log],
      leaderCommit: Int
  )
  case class AppendEntriesResult(term: Int, success: Boolean, entries: AppendEntries)

  case class RequestVote(
      term: Int,
      candidate: ActorRef,
      lastLogIndex: Int,
      lastLogTerm: Int
  )
  case class RequestVoteResult(
      term: Int,
      voteGranted: Boolean
  )

  case class Command(value: Int)
  case class Accepted(cmd: Command)

}
