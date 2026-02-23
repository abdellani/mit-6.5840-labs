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
		rf._resetVote()
	}
	rf.CurrentTerm = term
	if rf.Status != STATUS_FOLLOWER {
		rf.Log("downgrade to follower t= %d", rf.CurrentTerm)
	}
	rf.Status = STATUS_FOLLOWER
}

func (rf *Raft) _resetVote() {
	rf.VotedFor = -1
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
	rf.Log("promote to a candidate t=%d", rf.CurrentTerm)
	rf._resetElectionsTimeout()
}
func (rf *Raft) _promoteToLeader() {
	if rf.Status == STATUS_FOLLOWER {
		log.Panic("cannot promote a follower to a leader")
	}
	rf.Status = STATUS_LEADER
	rf.Log("promoting to leader (t=%d)", rf.CurrentTerm)
	go rf.HeartbeatLoop(rf.CurrentTerm)
}

func (rf *Raft) IsMajority(count int) bool {
	return count > len(rf.peers)/2
}

func (rf *Raft) Log(format string, args ...any) {
	if rf.Debug == false {
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
