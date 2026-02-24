package raft

import (
	"log"
	"math"
	"slices"
	"time"
)

type AppendEntryArg struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArg, reply *AppendEntryReply) {
	rf.Log("hb <= %d (t=%d pri=%d prt=%d le=%d lci=%d)", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
	rf._logState()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf._logState()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	if rf.CurrentTerm < args.Term {
		rf._becomeFollower(args.Term)
	}
	rf._resetElectionsTimeout()

	if !rf._doesEntryExist(args.PrevLogIndex, args.PrevLogTerm) {

		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	rf._aeLogsMatching(args)

	reply.Success = true
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, rf._lastEntryIndex())
	}
}

func (rf *Raft) _doesEntryExist(index, term int) bool {
	if index > rf._lastEntryIndex() {
		return false
	}
	return rf._termAt(index) == term
}

func (rf *Raft) _aeLogsMatching(args *AppendEntryArg) {
	for entryIndex := 0; entryIndex < len(args.Entries); entryIndex++ {
		logIndex := args.PrevLogIndex + entryIndex + 1
		if rf._lastEntryIndex() < logIndex {
			rf._appendEntries(args.Entries[entryIndex:])
			return
		}
		entryTerm := args.Entries[entryIndex].Term
		logsTerm := rf.Logs[logIndex].Term
		if logsTerm == entryTerm {
			continue
		}
		rf._truncateLogsFrom(logIndex)
		rf._appendEntries(args.Entries[entryIndex:])
		return
	}
}
func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	if !rf._isLeader() {
		rf.mu.Unlock()
		return
	}
	term := rf.CurrentTerm

	rf.mu.Unlock()

	followerReplicator := func(peer int) {
		rf.Log("hb => %d", peer)
		rf.mu.Lock()
		if !rf._isLeader() {
			rf.mu.Unlock()
			return
		}
		prevLogIndex := rf.NextIndex[peer] - 1
		prevLogTerm := rf._termAt(prevLogIndex)
		entries := rf._entriesFrom(prevLogIndex + 1)
		rf.mu.Unlock()
		args := AppendEntryArg{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.CommitIndex,
		}
		reply := AppendEntryReply{}

		ok := rf.sendAppendEntry(peer, &args, &reply)
		rf.Log("hb => %d (ok? %v)", peer, ok)

		if rf.killed() {
			return
		}
		if !ok {
			return
		}

		rf.mu.Lock()
		if !rf._isLeader() {
			rf.mu.Unlock()
			return
		}
		if rf.CurrentTerm < reply.Term {
			rf._becomeFollower(reply.Term)
			rf._resetElectionsTimeout()
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			if prevLogIndex+len(entries) < rf.MatchIndex[peer] {
				log.Panic("Match should never decreate !")
			}
			rf.MatchIndex[peer] = prevLogIndex + len(entries)
			rf.NextIndex[peer] = prevLogIndex + len(entries) + 1
		} else {
			rf.NextIndex[peer] = prevLogIndex
		}

		rf._checkCI()
		rf.mu.Unlock()
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go followerReplicator(i)
	}
}

func (rf *Raft) _checkCI() {
	match := make([]int, len(rf.MatchIndex))
	copy(match, rf.MatchIndex)
	rf.Log("nextIndex %v", rf.NextIndex)
	rf.Log("matchIndex %v", rf.MatchIndex)
	match[rf.me] = rf._lastEntryIndex()
	slices.Sort(match)

	// hisim =  Heighest Index Shared In Majority
	// the majority of node have at least "hisim" entries in their logs
	middle := int(math.Floor(float64(len(rf.peers)) / 2))
	hisim := match[middle]
	term := rf._termAt(hisim)
	if term != rf.CurrentTerm {
		// don't commit previous term
		return
	}
	rf.CommitIndex = hisim
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArg, reply *AppendEntryReply) bool {
	done := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
		done <- ok
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(120 * time.Millisecond):
		return false
	}
	// return rf.peers[server].Call("Raft.AppendEntry", args, reply)
}
