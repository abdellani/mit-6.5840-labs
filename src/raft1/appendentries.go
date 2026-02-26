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
	Term      int
	Success   bool
	HintIndex int
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

	rf._becomeFollower(args.Term)
	rf._resetElectionsTimeout()

	if !rf._doesEntryExist(args.PrevLogIndex, args.PrevLogTerm) {

		reply.Term = rf.CurrentTerm
		reply.Success = false
		if rf._lastEntryIndex() < args.PrevLogIndex {
			reply.HintIndex = rf._lastEntryIndex() + 1
		} else {
			conflictingTerm := rf._termAt(args.PrevLogIndex)
			reply.HintIndex = rf._firstOccurence(conflictingTerm)
		}
		return
	}

	rf._aeLogsMatching(args)

	reply.Success = true
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, rf._lastEntryIndex())
	}
}

func (rf *Raft) _firstOccurence(term int) int {
	index := -1
	for i := 0; i <= rf._lastEntryIndex(); i++ {
		if rf._termAt(i) >= term {
			break
		}
		index = i
	}
	return index + 1
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
		logsTerm := rf._termAt(logIndex)
		if logsTerm == entryTerm {
			continue
		}
		rf._truncateLogsfromIndexAndAfter(logIndex)
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
	leaderId := rf.me
	rf.mu.Unlock()

	followerReplicator := func(peer int) {
		// rf.Log("hb => %d", peer)
		rf.mu.Lock()
		if !rf._isLeader() {
			rf.mu.Unlock()
			return
		}
		prevLogIndex := rf.NextIndex[peer] - 1

		if prevLogIndex >= rf.SnapshotData.LastIndex {
			prevLogTerm := rf._termAt(prevLogIndex)
			entries := rf._entriesFrom(prevLogIndex + 1)
			commitIndex := rf.CommitIndex
			rf.mu.Unlock()
			rf.sendAppendEntry(term, leaderId, prevLogIndex, prevLogTerm, entries, commitIndex, peer)
			return
		}
		rf.mu.Unlock()

		rf.sendInstallSnapshot(term, leaderId, rf.SnapshotData, peer)
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go followerReplicator(i)
	}
}

func (rf *Raft) sendAppendEntry(term, leaderId, prevLogIndex, prevLogTerm int, entries []Log, commitIndex int, peer int) {
	args := AppendEntryArg{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
	reply := AppendEntryReply{}

	ok := rf.sendAppendEntryRPC(peer, &args, &reply)

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
		rf.NextIndex[peer] = reply.HintIndex
	}

	rf._checkCommitIdex()
	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(term, leaderId int, snapshot Snapshot, peer int) {
	args := InstallSnapshotArg{
		Term:              term,
		LeaderId:          leaderId,
		LastIncludedIndex: snapshot.LastIndex,
		LastIncludedTerm:  snapshot.LastTerm,
		Data:              snapshot.Data,
	}
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshotRPC(peer, &args, &reply)

	if rf.killed() {
		return
	}
	if !ok {
		return
	}
	rf.mu.Lock()
	if rf.CurrentTerm < reply.Term {
		rf._becomeFollower(reply.Term)
		rf.mu.Unlock()
		return
	}
	rf.NextIndex[peer] = snapshot.LastIndex + 1
	rf.MatchIndex[peer] = snapshot.LastIndex
	rf.mu.Unlock()
}

func (rf *Raft) _checkCommitIdex() {
	match := make([]int, len(rf.MatchIndex))
	copy(match, rf.MatchIndex)
	rf.Log("nextIndex %v", rf.NextIndex)
	rf.Log("matchIndex %v", rf.MatchIndex)
	match[rf.me] = rf._lastEntryIndex()
	rf.Log("match %v", match)

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
	rf.Log("ci %d -> %d", rf.CommitIndex, hisim)
	rf.CommitIndex = hisim
}

func (rf *Raft) sendAppendEntryRPC(server int, args *AppendEntryArg, reply *AppendEntryReply) bool {
	done := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
		done <- ok
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(90 * time.Millisecond):
		return false
	}
	// return rf.peers[server].Call("Raft.AppendEntry", args, reply)
}

type InstallSnapshotArg struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArg, reply *InstallSnapshotReply) {
	rf.Log("<-InstallSnapShot %d (t=%d li=%d lt=%d)", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf._logState()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		return
	}
	if rf.CurrentTerm < args.Term {
		rf._becomeFollower(args.Term)
	}
	rf.Logs = []Log{}
	rf._installSnapShot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	reply.Term = rf.CurrentTerm
}

func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArg, reply *InstallSnapshotReply) bool {
	done := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		done <- ok
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(90 * time.Millisecond):
		return false
	}
}
