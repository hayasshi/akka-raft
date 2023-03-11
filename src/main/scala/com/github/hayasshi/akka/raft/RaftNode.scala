package com.github.hayasshi.akka.raft

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import com.github.hayasshi.akka.raft.Leader.SendHeartbeat
import com.github.hayasshi.akka.raft.RaftProtocol._

import scala.collection.mutable.{Map => MutMap}
import scala.concurrent.duration.DurationInt
import scala.util.Random

object RaftNode {
  case class Start(members: Set[ActorRef])
}

trait RaftNode { me: Actor with ActorLogging =>

  // Persistent state on all services:
  var currentTerm: Int = 0
  var votedFor: Option[ActorRef] = None
  var logs: IndexedSeq[Log] = IndexedSeq(Log(0, 0, 0)) // Dummy in initial logs
  var state: Int = 0

  // Volatile state on all servers:
  var commitIndex: Int = 0
  var lastApplied: Int = 0

  def applyEntry(appliedIndex: Int): Unit = {
    state = state + logs.dropWhile(_.index <= lastApplied).takeWhile(_.index <= appliedIndex).map(_.value).sum
    lastApplied = appliedIndex
  }

  // Members
  var members: Set[ActorRef] = Set.empty

  // Volatile state on leader:
  var nextIndex: Map[ActorRef, Int]  = members.map(_ -> (logs.last.index + 1)).toMap
  var matchIndex: Map[ActorRef, Int] = members.map(_ -> logs.last.index).toMap

  def followerBehavior: Receive
  def candidateBehavior: Receive
  def leaderBehavior: Receive

  override def receive: Receive = {
    case msg: RaftNode.Start =>
      members = msg.members
      context.become(followerBehavior)
      log.info(s"Start raft member with $members")
      self ! Follower.FollowerStart
  }
}

object Follower {
  case object FollowerStart
  case object ElectionTimeout
  case class RedirectTo(leader: Option[ActorRef], requestedCommand: Command)
}

trait Follower { me: Actor with ActorLogging with RaftNode =>
  import Follower._

  var leader: Option[ActorRef] = None

  var electionTimeoutSchedule: Option[Cancellable] = None

  private def scheduleElectionTimeout(): Unit =
    electionTimeoutSchedule = Some(
      context.system.scheduler.scheduleOnce(Random.between(3000, 5000).millis, self, ElectionTimeout)(
        context.dispatcher
      )
    )

  private def cancelElectionTimeout(): Unit =
    electionTimeoutSchedule.foreach(_.cancel())

  def followerBehavior: Receive = {
    // 1. If term < currentTerm, return false
    case entries: AppendEntries if entries.term < currentTerm =>
      log.info(s"Follower AppendEntriesRPC return false: entries.term=${entries.term}, currentTerm=$currentTerm")
      entries.leader ! AppendEntriesResult(currentTerm, success = false, entries)

    case entries: AppendEntries =>
      // 2. Reset election timeout
      cancelElectionTimeout()

      // 3. If leaderId is not recognized
      if (leader.isEmpty || leader.exists(_ != entries.leader)) {
        leader = Some(entries.leader)
        votedFor = None
      }

      log.info(s"Follower AppendEntriesRPC receive $entries")

      // 4. If log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm, return false
      if (entries.prevLogIndex > logs.size || logs(entries.prevLogIndex).term != entries.prevLogTerm) {
        log.info(s"Follower AppendEntriesRPC return false: logs(entries.prevLogIndex).term=${logs(entries.prevLogIndex).term}")
        entries.leader ! AppendEntriesResult(currentTerm, success = false, entries)
      }
      // 5. If an existing entry conflicts with a new one (same index but different terms),
      // delete the existing entry and all that follow it
      // 6. Append any new entries not already in the log
      else {
        logs = logs
          .takeWhile(_.index <= entries.prevLogIndex)
          .appendedAll(entries.entries)

        // 7. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if (entries.leaderCommit > commitIndex) {
          commitIndex = math.min(entries.leaderCommit, logs.last.index)

          // apply entries
          applyEntry(commitIndex)
        }

        currentTerm = entries.term

        // 8. Return success
        leader.foreach(_ ! AppendEntriesResult(currentTerm, success = true, entries))
        log.info(s"Follower AppendEntriesRPC return true: commitIndex=$commitIndex, lastApplied=$lastApplied, state=$state")
      }

      scheduleElectionTimeout()

    case request: RequestVote =>
      cancelElectionTimeout()

      log.info(s"Follower RequestVoteRPC receive $request")

      val candidate = request.candidate

      def isAlreadyVoted: Boolean = votedFor.isEmpty || votedFor.contains(candidate)
      def logsUpToDate: Boolean   = request.lastLogIndex >= logs.last.index && request.lastLogTerm >= logs.last.term

      val result = if (request.term < currentTerm) {
        // 1. Reply false if term < currentTerm
        RequestVoteResult(currentTerm, voteGranted = false)

      } else if (isAlreadyVoted && logsUpToDate) {
        // 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
        currentTerm = request.term
        RequestVoteResult(currentTerm, voteGranted = true)
      
      } else {
        // 3. Reply false otherwise
        RequestVoteResult(currentTerm, voteGranted = false)
      }

      log.info(s"Follower RequestVoteRPC return $result")
      candidate ! result

      scheduleElectionTimeout()

    case ElectionTimeout =>
      log.info(s"Follower election timeout")
      context.become(candidateBehavior)
      self ! Candidate.StartElection

    case x: Command =>
      log.info(s"Follower redirect command: $x, leader=$leader")
      sender() ! RedirectTo(leader, x)
    
    case FollowerStart =>
      cancelElectionTimeout()
      scheduleElectionTimeout()
  }

}

object Candidate {
  case object StartElection
  case object CandidateTimeout
}

trait Candidate {
  me: Actor with ActorLogging with RaftNode =>
  import Candidate._

  val requestedMembers: MutMap[ActorRef, Int] = MutMap.empty

  var candidateTimeoutSchedule: Option[Cancellable] = None

  private def scheduleCandidateTimeout(): Unit =
    candidateTimeoutSchedule = Some(
      context.system.scheduler.scheduleOnce(Random.between(3000, 5000).millis, self, CandidateTimeout)(
        context.dispatcher
      )
    )

  private def cancelCandidateTimeout(): Unit =
    candidateTimeoutSchedule.foreach(_.cancel())

  def candidateBehavior: Receive = {
    case StartElection =>
      // increment term
      currentTerm += 1

      requestedMembers.clear()
      requestedMembers.addAll(members.map(_ -> 0))
      requestedMembers.addOne(self -> 1) // votes for itself

      val requestVote = RequestVote(currentTerm, self, commitIndex, logs.last.term)
      requestedMembers.keys.foreach(_ ! requestVote)

      log.info(s"Candidate start election: currentTerm=$currentTerm, requestedMembers=$requestedMembers")
      scheduleCandidateTimeout()

    case RequestVoteResult(term, voteGranted) if voteGranted && term == currentTerm =>
      cancelCandidateTimeout()
      log.info(s"Candidate receive voted result: term=$term")

      requestedMembers.update(sender(), 1) // Granted to 1
      val grantedCount = requestedMembers.values.sum
      if (grantedCount > requestedMembers.size / 2) {
        //  it receives votes from a majority of the servers, become to leader
        cancelCandidateTimeout()
        requestedMembers.clear()
        nextIndex  = members.map(_ -> (logs.last.index + 1)).toMap
        matchIndex = members.map(_ -> logs.last.index).toMap
        context.become(leaderBehavior)
        self ! SendHeartbeat
        log.info(s"Candidate become to leader: nextIndex=$nextIndex, matchIndex=$matchIndex")

      } else {
        scheduleCandidateTimeout()
      }

    case CandidateTimeout =>
      requestedMembers.clear()
      self ! StartElection
      log.info("Candidate election timeout")

    case entries: AppendEntries if entries.term >= currentTerm =>
      cancelCandidateTimeout()
      context.become(followerBehavior)
      self ! entries
      log.info(s"Candidate receive AppendEntries from other new leader: $entries")
  }
}

object Leader {
  case object SendHeartbeat
}

trait Leader {
  me: Actor with ActorLogging with RaftNode =>

  var nextSendHeartbeatSchedule: Option[Cancellable] = None

  def scheduleSendHeartbeat(): Unit =
    nextSendHeartbeatSchedule = Some(
      context.system.scheduler.scheduleOnce(1500.millis, self, SendHeartbeat)(
        context.dispatcher
      )
    )

  private def cancelSendHeartbeat(): Unit =
    nextSendHeartbeatSchedule.foreach(_.cancel())

  def leaderBehavior: Receive = {
    case SendHeartbeat =>
      members.foreach { member =>
        val prevLogIndex = matchIndex(member)
        val heartbeat = AppendEntries(
          currentTerm,
          self,
          logs(prevLogIndex).index,
          logs(prevLogIndex).term,
          IndexedSeq(), // empty entry
          commitIndex
        )
        member ! heartbeat
        log.info(s"Leader send heartbeat to $member: $heartbeat")
      }
      scheduleSendHeartbeat()

    case cmd: Command =>
      cancelSendHeartbeat()

      val newLog = Log(currentTerm, logs.last.index + 1, cmd.value)
      logs = logs :+ newLog
      log.info(s"Leader receive command from client: $newLog")

      members.foreach { member =>
        val prevLogIndex = matchIndex(member)
        val entries = AppendEntries(
          currentTerm,
          self,
          logs(prevLogIndex).index,
          logs(prevLogIndex).term,
          logs.dropWhile(_.index <= prevLogIndex),
          commitIndex
        )
        member ! entries
        log.info(s"Leader send AppendEntries log to $member: $entries")
      }

      sender() ! Accepted(cmd)

      scheduleSendHeartbeat()

    case result: AppendEntriesResult if result.success =>
      log.info(s"Leader receive AppendEntriesResult: $result")

      // only no hearbeat response
      if (result.entries.entries.nonEmpty) {
        // If successful: update nextIndex and matchIndex for follower (§5.3)
        val follower = sender()
        val logIndex = result.entries.entries.last.index
        nextIndex  = nextIndex.updated(follower, logIndex + 1)
        matchIndex = matchIndex.updated(follower, logIndex)

        // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
        val majorityNumber  = members.size / 2 // only followers
        val replicatedIndex = matchIndex.values.toVector.sortWith(_ > _).take(majorityNumber).min // minimal replicated log index in majority committed logs
        if (replicatedIndex > commitIndex) {
          commitIndex = replicatedIndex
          applyEntry(replicatedIndex)
          log.info(s"Leader commit logs: commitIndex=$commitIndex, lastApplied=$lastApplied, state=$state")
        }
      }
    
    case result: AppendEntriesResult if !result.success =>
      // TODO: If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
      log.error(s"Leader receive AppendEntriesResult=false: $result")

    case entries: AppendEntries if entries.term > currentTerm =>
      cancelSendHeartbeat()
      context.become(followerBehavior)
      self ! entries
      log.info(s"Leader receive AppendEntries from new leader and become to follower: $entries")
  }
}
