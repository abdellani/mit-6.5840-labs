package raft

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

const (
	STATUS_FOLLOWER  = 0
	STATUS_CANDIDATE = 1
	STATUS_LEADER    = 2
)

func (rf *Raft) _becomeFollower(term int) {
	if term < rf.CurrentTerm {
		log.Panic("vote term can only increase")
	}
	if term > rf.CurrentTerm {
		rf._resetVoteFlag()
	}
	rf.CurrentTerm = term
	if rf.Status != STATUS_FOLLOWER {
		rf.Log("downgrade to follower t= %d", rf.CurrentTerm)
	}
	if rf.Status == STATUS_FOLLOWER {
		return
	}
	rf.Status = STATUS_FOLLOWER
	rf.persist()
}

func (rf *Raft) _resetVoteFlag() {
	if rf.VotedFor == -1 {
		return
	}
	rf.VotedFor = -1
	rf.persist()
	rf.Log("reset voteFor")
}

func (rf *Raft) _voteFor(candidateId int) {
	if rf.VotedFor == candidateId {
		return
	}
	if rf.VotedFor != -1 {
		log.Panic("one vote per term!")
	}
	rf.VotedFor = candidateId
	rf.persist()
	rf.Log("v for %d (t=%d)", candidateId, rf.CurrentTerm)
}

func (rf *Raft) _resetElectionsTimeout() {
	ms := 400 + (rand.Int63() % 400)
	duration := time.Duration(ms) * time.Millisecond
	rf.ElectionTimeout = time.Now().Add(duration)
	rf.Log("reset Election Timeout to %s", FormatTime(rf.ElectionTimeout))
}

func (rf *Raft) _reachedTimeForElections() bool {
	return time.Now().After(rf.ElectionTimeout)
}

func (rf *Raft) _promoteToCandidate() {
	if rf.Status == STATUS_LEADER {
		log.Panic("cannot promote a leader to a candidate")
	}
	rf.Status = STATUS_CANDIDATE
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
	rf.Log("promote to a candidate t=%d", rf.CurrentTerm)
	rf._resetElectionsTimeout()
}
func (rf *Raft) _promoteToLeader() {
	if rf.Status == STATUS_FOLLOWER {
		log.Panic("cannot promote a follower to a leader")
	}
	rf.Status = STATUS_LEADER
	lastIndex := rf._lastEntryIndex()
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex[i] = lastIndex + 1
		rf.MatchIndex[i] = -1
	}
	rf.Log("promoting to leader (t=%d)", rf.CurrentTerm)
}

func (rf *Raft) IsMajority(count int) bool {
	return count > len(rf.peers)/2
}

func (rf *Raft) Log(format string, args ...any) {
	if Debug == false {
		return
	}
	now := time.Now()
	formatted := FormatTime(now)
	message := fmt.Sprintf(format, args...)
	fmt.Println(formatted, " - ", rf.me, " : ", message)
}
func FormatTime(t time.Time) string {
	return t.Format("2006-01-02 15:04:05.000")
}

func (rf *Raft) _isLeader() bool {
	return rf.Status == STATUS_LEADER
}

func (rf *Raft) _lastEntryIndex() int {
	return len(rf.Logs) + rf.SnapshotData.LastIndex
}

func (rf *Raft) _lastEntryTerm() int {
	if len(rf.Logs) == 0 {
		return rf.SnapshotData.LastTerm
	}
	return rf.Logs[len(rf.Logs)-1].Term
}

func (rf *Raft) _termAt(index int) int {
	if index == -1 {
		return -1
	}
	if index < rf.SnapshotData.LastIndex {
		return -1
	}
	if index == rf.SnapshotData.LastIndex {
		return rf.SnapshotData.LastTerm
	}
	logsIndex := rf._logIndex(index)
	return rf.Logs[logsIndex].Term
}

func (rf *Raft) _appendEntries(entries []Log) {
	for i := 0; i < len(entries); i++ {
		rf._appendEntry(entries[i])
	}
}

func (rf *Raft) _appendEntry(entry Log) {
	if entry.Term < rf._lastEntryTerm() {
		log.Panic("terms should only increase on the logs")
	}
	rf.Logs = append(rf.Logs, entry)
	rf.persist()
}

func (rf *Raft) _truncateLogsfromIndexAndAfter(index int) {
	logIndex := rf._logIndex(index)
	rf.Logs = rf.Logs[:logIndex]
	rf.persist()
}
func (rf *Raft) _logState() {
	rf.Log("t=%d s=%d lei=%d(%d) let=%d ci=%d sli=%d slt=%d", rf.CurrentTerm,
		rf.Status, rf._lastEntryIndex(), len(rf.Logs), rf._lastEntryTerm(),
		rf.CommitIndex, rf.SnapshotData.LastIndex,
		rf.SnapshotData.LastTerm)
}

func (rf *Raft) _entriesFrom(index int) []Log {
	if index < rf.SnapshotData.LastIndex {
		log.Panic("_entriesFrom: logIndex refers to an index that's stored on snapshot")
	}
	logIndex := rf._logIndex(index)
	subarray := make([]Log, len(rf.Logs[logIndex:]))
	copy(subarray, rf.Logs[logIndex:])
	return subarray
}

// convert command index to index in logs
func (rf *Raft) _logIndex(index int) int {

	if rf.SnapshotData.LastIndex == -1 {
		//no snapshotinstalled
		return index
	}

	return index - rf.SnapshotData.LastIndex - 1
}

func (rf *Raft) _truncateLogsBefore(index int) {
	logIndex := rf._logIndex(index)
	if logIndex > len(rf.Logs)-1 {
		return
	}
	rf.Logs = rf.Logs[logIndex+1:]
}

func (rf *Raft) _getLogEntry(index int) Log {
	logIndex := rf._logIndex(index)

	return rf.Logs[logIndex]
}
