package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	Debug           bool
	Status          int
	CurrentTerm     int
	ElectionTimeout time.Time
	VotedFor        int
	Logs            []Log
	CommitIndex     int
	NextIndex       []int
	MatchIndex      []int
	ApplyIndex      int
	ApplyCh         chan raftapi.ApplyMsg
}

type Log struct {
	Term    int
	Command any
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	isleader = rf._isLeader()
	term = rf.CurrentTerm
	rf.mu.Unlock()
	rf.Log("is leader? %v, t=%d", isleader, term)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		log.Panic("error while decoding")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = voteFor
		rf.Logs = logs
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.CurrentTerm
	index := -1
	isLeader := rf._isLeader()
	if isLeader {
		rf.Log("adding cmd %s", command)
		newEntry := Log{
			Term:    term,
			Command: command,
		}
		rf._appendEntry(newEntry)
		index = rf._lastEntryIndex() + 1
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.Log("killed!")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer func() { ticker.Stop() }()
	for rf.killed() == false {
		<-ticker.C
		rf.mu.Lock()
		if rf._isLeader() {
			rf.mu.Unlock()
			rf.broadcastHeartbeat()
			continue
		}
		if rf._reachedTimeForElections() {
			rf.mu.Unlock()
			rf.RunElections()
			continue
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (3A, 3B, 3C).
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Status = STATUS_FOLLOWER
	rf._resetElectionsTimeout()
	rf.CommitIndex = -1
	rf.ApplyIndex = -1
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.ApplyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.Log("start!")
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLoop()
	return rf
}

func (rf *Raft) applyLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer func() { ticker.Stop() }()
	for !rf.killed() {
		<-ticker.C
		rf.mu.Lock()
		if rf.ApplyIndex == rf.CommitIndex {
			rf.mu.Unlock()
			continue
		}
		if rf.ApplyIndex > rf.CommitIndex {
			log.Panic("applyIndex should never exceed CI")
		}
		var messages []raftapi.ApplyMsg
		for rf.ApplyIndex < rf.CommitIndex {
			rf.ApplyIndex++
			entry := rf.Logs[rf.ApplyIndex]
			cmdindex := rf.ApplyIndex + 1
			messages = append(messages, raftapi.ApplyMsg{
				Command:      entry.Command,
				CommandIndex: cmdindex,
				CommandValid: true,
			})
		}

		rf.mu.Unlock()

		for _, message := range messages {
			rf.ApplyCh <- message
		}

	}
}
