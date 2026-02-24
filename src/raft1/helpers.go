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
	return len(rf.Logs) - 1
}
func (rf *Raft) _lastEntryTerm() int {
	if rf._lastEntryIndex() == -1 {
		return -1
	}
	return rf.Logs[rf._lastEntryIndex()].Term
}

func (rf *Raft) _termAt(index int) int {
	if index == -1 {
		return -1
	}
	return rf.Logs[index].Term
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

func (rf *Raft) _truncateLogsFrom(index int) {
	rf.Logs = rf.Logs[:index]
	rf.persist()
}
func (rf *Raft) _logState() {
	rf.Log("t=%d lei=%d let=%d ci=%d", rf.CurrentTerm, rf._lastEntryIndex(), rf._lastEntryTerm(), rf.CommitIndex)
}

func (rf *Raft) _entriesFrom(index int) []Log {
	subarray := make([]Log, len(rf.Logs[index:]))
	copy(subarray, rf.Logs[index:])
	return subarray
}
