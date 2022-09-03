package com.github.hayasshi.akka.raft

import akka.actor.ActorRef

object RaftProtocol {

  case class Log[A](
      term: Int,
      index: Int,
      value: A
  )

  case class AppendEntries[A](
      term: Int,
      leader: ActorRef,
      prevLogIndex: Int,
      prevLogTerm: Int,
      entries: IndexedSeq[Log[A]],
      leaderCommit: Int
  )
  case class AppendEntriesResult(term: Int, success: Boolean)

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

}
