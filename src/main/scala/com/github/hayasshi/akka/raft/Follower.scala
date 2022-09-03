package com.github.hayasshi.akka.raft

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import com.github.hayasshi.akka.raft.Leader.SendHeartbeat
import com.github.hayasshi.akka.raft.RaftProtocol._

import scala.collection.mutable.{Map => MutMap}
import scala.concurrent.duration.DurationInt

trait RaftBase { self: Actor =>
  type StateType
  type EntryType

  // Persistent state on all services:
  var currentTerm: Int = 0
  var votedFor: Option[ActorRef] = None
  var logs: IndexedSeq[Log[EntryType]] = IndexedSeq.empty

  // Volatile state on all servers:
  var commitIndex: Int = 0
  var lastApplied: Int = 0

  // Volatile state on leader:
  def nextIndex: Map[ActorRef, Int]
  def matchIndex: Map[ActorRef, Int]

  // Members
  var members: Set[ActorRef]
}

object Follower {
  case object ElectionTimeout
  case class RedirectTo[X](leader: ActorRef, requestedCommand: X)
}

trait Follower { me: Actor with ActorLogging with RaftBase with Candidate =>
  import Follower._

  var leader: ActorRef = _

  def currentState: State[StateType]
  def applyEntry[NewState >: StateType](
      state: State[StateType],
      entry: AppendEntries[EntryType]
  ): State[NewState]

  var electionTimeoutSchedule: Option[Cancellable] = None

  private def scheduleElectionTimeout(): Unit =
    electionTimeoutSchedule = Some(
      context.system.scheduler.scheduleOnce(150.millis, self, ElectionTimeout)(
        context.dispatcher
      )
    )

  private def cancelElectionTimeout(): Unit =
    electionTimeoutSchedule.foreach(_.cancel())

  def followerBehavior: Receive = {
    case entries: AppendEntries[EntryType] =>
      cancelElectionTimeout()

      if (entries.term < currentTerm)
        entries.leader ! AppendEntriesResult(currentTerm, success = false)
      else if (logs.last.index < entries.prevLogIndex)
        entries.leader ! AppendEntriesResult(currentTerm, success = false)
      else {
        this.leader = entries.leader
        logs = logs
          .takeWhile(_.index <= entries.prevLogIndex)
          .appendedAll(entries.entries) // ss5.3
        if (entries.leaderCommit > commitIndex) {
          commitIndex = math.min(entries.leaderCommit, logs.last.index)
        }
        leader ! AppendEntriesResult(currentTerm, success = true)
      }

      scheduleElectionTimeout()

    case request: RequestVote =>
      cancelElectionTimeout()

      val candidate = request.candidate
      if (request.term < currentTerm)
        candidate ! RequestVoteResult(currentTerm, voteGranted = false)
      else if (votedFor.isDefined && votedFor.exists(_ != candidate))
        candidate ! RequestVoteResult(currentTerm, voteGranted = false)
      else
        candidate ! RequestVoteResult(currentTerm, voteGranted = true)

      scheduleElectionTimeout()

    case ElectionTimeout =>
      context.become(candidateBehavior)
      self ! Candidate.StartElection

    case x =>
      sender() ! RedirectTo(leader, x)
  }

}

object Candidate {
  case object StartElection
  case object CandidateTimeout
}

trait Candidate {
  me: Actor with ActorLogging with RaftBase with Leader with Follower =>
  import Candidate._

  val requestedMembers: MutMap[ActorRef, Int] =
    MutMap.empty

  var candidateTimeoutSchedule: Option[Cancellable] = None

  private def scheduleCandidateTimeout(): Unit =
    candidateTimeoutSchedule = Some(
      context.system.scheduler.scheduleOnce(150.millis, self, CandidateTimeout)(
        context.dispatcher
      )
    )

  private def cancelCandidateTimeout(): Unit =
    candidateTimeoutSchedule.foreach(_.cancel())

  def candidateBehavior: Receive = {
    case StartElection =>
      // increment term
      currentTerm += 1

      val requestVote =
        RequestVote(currentTerm, self, commitIndex, logs.last.term)
      requestedMembers.addAll(members.map(_ -> 0))
      requestedMembers.keys.foreach(_ ! requestVote)

      scheduleCandidateTimeout()

    case RequestVoteResult(term, voteGranted)
        if voteGranted && term == currentTerm =>
      requestedMembers.update(sender(), 1) // Granted to 1
      val grantedCount = requestedMembers.values.sum
      if (grantedCount > requestedMembers.size / 2) {
        // TODO: become to leader
        cancelCandidateTimeout()
        requestedMembers.clear()
        context.become(leaderBehavior)
        self ! SendHeartbeat
      }

    case CandidateTimeout =>
      requestedMembers.clear()
      self ! StartElection

    case entries: AppendEntries[EntryType] =>
      // TODO: check entries term, commit, etc..., if ok then become follower
      context.become(followerBehavior)

  }
}

object Leader {
  case object SendHeartbeat
}

trait Leader {
  me: Actor with ActorLogging with RaftBase with Follower =>

  val nextIndex: MutMap[ActorRef, Int] = MutMap.empty

  val matchIndex: MutMap[ActorRef, Int] = MutMap.empty

  var nextSendHeartbeatSchedule: Option[Cancellable] = None

  def scheduleSendHeartbeat(): Unit =
    nextSendHeartbeatSchedule = Some(
      context.system.scheduler.scheduleOnce(50.millis, self, SendHeartbeat)(
        context.dispatcher
      )
    )

  def leaderBehavior: Receive = {
    case SendHeartbeat =>
      val heartbeat = AppendEntries(
        currentTerm,
        self,
        logs.size - 1,
        logs.last.term,
        IndexedSeq(), // empty entry
        commitIndex
      )
      members.foreach(_ ! heartbeat)
      scheduleSendHeartbeat()

    case cmd: EntryType =>
      commitIndex += 1
      val newLog = Log(currentTerm, commitIndex, cmd)
      val entries = AppendEntries(
        currentTerm,
        self,
        logs.size - 1,
        logs.last.term,
        IndexedSeq(newLog),
        commitIndex
      )
      logs = logs :+ newLog
      members.foreach(_ ! entries)

    case result: AppendEntriesResult =>
      // TODO 定数足満たした場合は commit ?
      println(result)

    case entries: AppendEntries[EntryType] =>
      // TODO 他のノードから AppendEntries が送られてきた場合は、各チェックをおこなった上で Follower に遷移
      context.become(followerBehavior)
      self ! entries
  }
}
