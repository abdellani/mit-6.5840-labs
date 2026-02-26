package raft

import (
	"bytes"
	"log"

	"6.5840/labgob"
)

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
	e.Encode(rf.SnapshotData.LastIndex)
	e.Encode(rf.SnapshotData.LastTerm)
	raftstate := w.Bytes()
	if rf.SnapshotData.LastIndex == -1 {
		rf.persister.Save(raftstate, nil)
		return
	}
	rf.persister.Save(raftstate, rf.SnapshotData.Data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:

	if rf._lastEntryTerm() < rf.SnapshotData.LastTerm {
		log.Panic("last entry term should never be small to the last term in the snapshot")
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []Log
	var lastIndex int
	var lastTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIndex) != nil ||
		d.Decode(&lastTerm) != nil {
		log.Panic("error while decoding")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = voteFor
		rf.Logs = logs
		rf.SnapshotData.LastIndex = lastIndex
		rf.SnapshotData.LastTerm = lastTerm
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.SnapshotData.Data = data
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index-- // we use commandIndex - 1, internally

	if index < rf.SnapshotData.LastIndex {
		log.Panic("New Snapshot is less update to date.")
	}
	term := rf._termAt(index)
	rf.Log("snapshot (li=%d lt=%d)", index, term)
	rf._truncateLogsBefore(index)
	rf._installSnapShot(index, term, snapshot)
}

func (rf *Raft) _installSnapShot(lastIndex, lastTerm int, data []byte) {
	rf.SnapshotData.LastIndex = lastIndex
	rf.SnapshotData.LastTerm = lastTerm
	rf.SnapshotData.Data = data
	rf.persist()
}
