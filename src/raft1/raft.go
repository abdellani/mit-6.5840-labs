package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	STATUS_FOLLOWER  = 0
	STATUS_CANDIDATE = 1
	STATUS_LEADER    = 2
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
	Status                       int
	ElectionsTimerStartReference int64
	CurrentTerm                  int
	VotedFor                     int
	Logs                         Logs

	CommitIndex int
	LastApplied int

	NextIndex  []int
	MatchIndex []int

	ApplyChBuffer chan raftapi.ApplyMsg
	ApplyCh       chan raftapi.ApplyMsg
}

type Log struct {
	Term    int
	Command any
}

type Logs struct {
	Logs           []Log
	LastEntryIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int = rf.CurrentTerm
	var isleader bool = rf.Status == STATUS_LEADER
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
	// rf.Log("persisting ...")
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
	var votedFor int
	logs := Logs{}
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Panic("failed decoding saved state")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf._logAndReleaseLock()

	rf.Log(fmt.Sprintf("receive vote req from %d (t= %d lli=%d llt=%d)", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm))
	rf._logState()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		rf.Log(fmt.Sprintf("vote rejected for %d", args.CandidateId))
		return
	}

	if rf.CurrentTerm < args.Term {
		rf._becomeFollower(args.Term)
		reply.Term = args.Term
		if rf._isPeerLogUptodate(args.LastLogIndex, args.LastLogTerm) {
			reply.VoteGranted = true
			rf._voteFor(args.CandidateId)
			rf._resetElectionsTimeout()
			rf.Log(fmt.Sprintf("voted for %d", args.CandidateId))
		} else {
			reply.VoteGranted = false
			rf.Log(fmt.Sprintf("vote rejected for %d, logs not up-to-date", args.CandidateId))
		}
		rf.persist()
		return
	}

	// rf.CurrentTerm == args.Term
	reply.Term = rf.CurrentTerm
	// if voted for the same candidate on same term (message dropped for example)
	if rf.VotedFor == args.CandidateId {
		reply.VoteGranted = true
		rf._resetElectionsTimeout()
		return
	}

	if rf.VotedFor != -1 {
		//already voted
		rf.Log(fmt.Sprintf("vote rejected for %d (already voted)", args.CandidateId))
		reply.VoteGranted = false
		return
	}

	// rf.CurrentTerm == args.Term && rf.VotedFor == -1
	if rf._isPeerLogUptodate(args.LastLogIndex, args.LastLogTerm) {
		rf._voteFor(args.CandidateId)
		rf.Log(fmt.Sprintf("voted for %d", args.CandidateId))
		reply.VoteGranted = true
		rf._resetElectionsTimeout()
		rf.persist()
	} else {
		reply.VoteGranted = false
		rf.Log(fmt.Sprintf("vote rejected for %d, log not up-to-date", args.CandidateId))
	}
}

func (rf *Raft) _voteFor(peer int) {
	rf.VotedFor = peer
}

func (rf *Raft) _isPeerLogUptodate(peerLastLogIndex, peerLastLogTerm int) bool {

	if rf.Logs.LastEntryIndex < 0 {
		// local log empty
		return true
	}
	localLastLogIndex := rf.Logs.LastEntryIndex
	localLastLogTerm := rf.Logs._lastEntryTerm()

	if localLastLogTerm != peerLastLogTerm {
		return localLastLogTerm < peerLastLogTerm
	}

	// localLastLogTerm == peerLastLogTerm
	return localLastLogIndex <= peerLastLogIndex

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	for range 3 {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			return ok
		}
	}
	return false
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.Status == STATUS_LEADER
	term := rf.CurrentTerm
	if !isLeader {
		return -1, term, isLeader
	}
	rf.Logs.LastEntryIndex++
	index := rf.Logs.LastEntryIndex
	rf.Logs.Logs[index] = Log{
		Term:    term,
		Command: command,
	}
	rf.persist()
	return index + 1, term, isLeader
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
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		if rf.ShouldTriggerVote() {
			rf.RunVote()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf := &Raft{
		NextIndex:  make([]int, len(peers)),
		MatchIndex: make([]int, len(peers)),
		Logs: Logs{
			LastEntryIndex: -1,
			Logs:           make([]Log, 10000), // TODO: allocate memory dynamically
		},
		ApplyCh:       applyCh,
		ApplyChBuffer: make(chan raftapi.ApplyMsg, 50),
		CommitIndex:   -1,
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.Status = STATUS_FOLLOWER
	rf.VotedFor = -1
	rf.ElectionsTimerStartReference = time.Now().UnixMilli()
	go rf._commandsSendingLog()
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	rf.Log("started!")
	go rf.ticker()

	return rf
}

func (rf *Raft) RunVote() {
	rf.mu.Lock()

	rf._transitToCandidate()
	rf.persist()
	voteTerm := rf.CurrentTerm
	lastLogIndex := rf.Logs.LastEntryIndex
	lastLogTerm := rf.Logs._lastEntryTerm()
	rf.Log(fmt.Sprint("Running Vote: t= ", voteTerm))
	rf.Log(fmt.Sprintf("vt=%d llt=%d llt=%d", voteTerm, lastLogIndex, lastLogTerm))
	rf._logState()

	rf.mu.Unlock()
	var wg sync.WaitGroup
	votes := 1
	replies := make(chan RequestVoteReply, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			arg := RequestVoteArgs{
				Term:         voteTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(i, &arg, &reply)
			if !ok {
				return
			}
			rf.Log(fmt.Sprintf("vt: %d; received res from %d", voteTerm, i))
			replies <- reply
		}(i)
	}
	go func() {
		wg.Wait()
		close(replies)
	}()

	for reply := range replies {
		rf.mu.Lock()
		status := rf.Status
		currentTerm := rf.CurrentTerm
		if rf.killed() {
			return
		}
		if currentTerm != voteTerm {
			rf.Log("Current term is different from Vote term, break vote loop")
			rf.mu.Unlock()
			break
		}
		if status != STATUS_CANDIDATE {
			rf.Log("election, status is no longer candidate, breaking loop")
			rf.mu.Unlock()
			break
		}
		if reply.Term == voteTerm && reply.VoteGranted {
			votes++
			if rf.IsQuorum(votes) {
				rf.Log("reached quorum, promoting to leader")
				rf._promoteToLeader()
				//voteFor cleared, need to persiste
				rf.persist()
				rf.mu.Unlock()
				break
			}
		} else if reply.Term > voteTerm {
			voteTerm = reply.Term
			rf._becomeFollower(voteTerm)
			rf.persist()
			rf.mu.Unlock()
			rf.Log("discovered a new term during vote, canceling election")
			break
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) IsQuorum(votes int) bool {
	return votes > int(math.Floor(float64(len(rf.peers))/2))
}

func (rf *Raft) ShouldTriggerVote() bool {
	now := time.Now().UnixMilli()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return (rf.Status == STATUS_FOLLOWER || rf.Status == STATUS_CANDIDATE) && (now-rf.ElectionsTimerStartReference) > 400
}

// add LastEntryIndex as parameter
func (rf *Raft) HeartBeatsLoop(term int) {
	rf.mu.Lock()
	for i := range len(rf.peers) {
		rf.NextIndex[i] = rf.Logs.LastEntryIndex + 1
		rf.MatchIndex[i] = -1
	}

	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		currentTerm := rf.CurrentTerm
		currentStatus := rf.Status
		rf.mu.Unlock()
		if currentTerm != term {
			return
		}
		if currentStatus != STATUS_LEADER {
			return
		}
		// rf.Log("sending  heartbeats")
		var wg sync.WaitGroup
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go func(server int) {
				defer wg.Done()
				rf.mu.Lock()
				nextIndex := rf.NextIndex[server]
				lastEntryIndex := rf.Logs.LastEntryIndex
				entries := rf.Logs._copyStartingFrom(nextIndex)
				prevLogIndex := nextIndex - 1
				prevLogTerm := -1
				if prevLogIndex >= 0 {
					prevLogTerm = rf.Logs.Logs[prevLogIndex].Term
				}
				commitIndex := rf.CommitIndex
				rf.mu.Unlock()
				arg := AppendEntryArgument{
					Term:         currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: commitIndex,
				}
				reply := AppendEntryReply{}
				ok := rf.SendAppendEntry(server, &arg, &reply)
				if !ok {
					return
				}
				if rf.killed() {
					return
				}
				rf.mu.Lock()
				if rf.CurrentTerm != currentTerm {
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.CurrentTerm {
					rf._becomeFollower(reply.Term)
					rf.persist()
				} else if reply.Success == true {
					rf.NextIndex[server] = lastEntryIndex + 1
					rf.MatchIndex[server] = lastEntryIndex
					//TODO avoid updating Commit index, if the new index is already commited
				} else if reply.Success == false {
					if nextIndex == rf.NextIndex[server] {
						rf.NextIndex[server] = reply.HintIndex
					}
					if rf.NextIndex[server] < 0 {
						log.Panic("should not reach this value")
					}
				}
				rf.mu.Unlock()
				if reply.Success == true {
					rf.UpdateCommitIndexAfterAppendEntry()
				}
			}(i)
		}
		//TODO what if some RPC calls take too much time? the heartbeat loop will become solwer.
		go func() {
			wg.Wait()
		}()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) UpdateCommitIndexAfterAppendEntry() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Status != STATUS_LEADER {
		return
	}
	highestIndexWithQurum := -1
	// TODO O(n**2) complexity, optimize this later
	for i := 0; i < len(rf.MatchIndex); i++ {
		indexI := rf.MatchIndex[i]
		if indexI <= highestIndexWithQurum {
			continue
		}
		count := 1 //count self, MatchIndex[me] == -1 always
		for j := 0; j < len(rf.MatchIndex); j++ {
			indexJ := rf.MatchIndex[j]
			if indexJ < indexI {
				continue
			}
			count++
			if rf.IsQuorum(count) {
				highestIndexWithQurum = indexI
				break
			}
		}
	}
	rf.Log(fmt.Sprint("matched ", rf.MatchIndex))
	rf.Log(fmt.Sprintf("highest term term with majority found %d ", highestIndexWithQurum))
	if highestIndexWithQurum < 0 {
		return
	}
	termOfHIWQ := rf.Logs.Logs[highestIndexWithQurum].Term
	if termOfHIWQ < rf.CurrentTerm {
		return
	}
	if highestIndexWithQurum == rf.CommitIndex {
		return
	}
	rf.Log(fmt.Sprintf("ci %d -> %d ", rf.CommitIndex, highestIndexWithQurum))
	start := rf.CommitIndex
	rf._sendCommandsFromLogs(start, highestIndexWithQurum)
	rf.CommitIndex = highestIndexWithQurum
}

func (rf *Raft) _sendCommandsFromLogs(currentCommitIndex, nextCommitIndex int) {
	for i := currentCommitIndex + 1; i <= nextCommitIndex; i++ {
		rf.ApplyChBuffer <- raftapi.ApplyMsg{
			Command:      rf.Logs.Logs[i].Command,
			CommandIndex: i + 1,
			CommandValid: true,
		}
	}

}

func (rf *Raft) _commandsSendingLog() {
	for !rf.killed() {
		cmd := <-rf.ApplyChBuffer
		rf.ApplyCh <- cmd
	}

}

type AppendEntryArgument struct {
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

func (rf *Raft) _becomeFollower(term int) {
	if term < rf.CurrentTerm {
		return
	}
	if rf.Status != STATUS_FOLLOWER {
		rf.Log("downgrading to follower")
	}
	rf.Status = STATUS_FOLLOWER
	if term > rf.CurrentTerm {
		rf.VotedFor = -1
		rf._resetElectionsTimeout()
	}
	rf.CurrentTerm = term
}

func (rf *Raft) _promoteToLeader() {
	if rf.Status == STATUS_FOLLOWER {
		rf.Log("can't become a leader")
		return
	}
	rf.Log("becoming leader !")
	rf.Status = STATUS_LEADER
	rf.VotedFor = -1
	go rf.HeartBeatsLoop(rf.CurrentTerm)
}

func (rf *Raft) _transitToCandidate() {
	rf.Status = STATUS_CANDIDATE
	rf.VotedFor = rf.me
	rf.CurrentTerm++
}

func (rf *Raft) SendAppendEntry(server int, args *AppendEntryArgument, reply *AppendEntryReply) bool {
	ok := true
	for range 3 {
		ok = rf.peers[server].Call("Raft.AppendEntry", args, reply)
		if ok == true {
			break
		}
	}

	return ok
}
func (rf *Raft) AppendEntry(args *AppendEntryArgument, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf._logAndReleaseLock()
	rf.Log(fmt.Sprintf("rec AppendEntry from %d[t=%d pli=%d plt=%d lci=%d le=%d]", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries)))
	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	// rf.CurrentTerm <= args.Term
	// convert to  follower
	reply.Term = rf.CurrentTerm
	rf._becomeFollower(args.Term)
	rf.persist()
	rf._resetElectionsTimeout()

	localPrevLogIndex := rf.Logs.LastEntryIndex

	if localPrevLogIndex < args.PrevLogIndex {
		// some entries are missing on the peer
		reply.HintIndex = rf.Logs.LastEntryIndex + 1
		reply.Success = false
		return
	}

	// rf.Logs has an item on args.PrevLogIndex
	if rf.Logs._termAt(args.PrevLogIndex) != args.PrevLogTerm {
		// conflict between beteen node and leader
		// erase entry that conflict and what comes after
		conflictingTerm := rf.Logs._termAt(args.PrevLogIndex)
		rf.Logs._moveIndexTo(args.PrevLogIndex - 1)
		reply.Success = false
		reply.HintIndex = rf.Logs._firstOccuranceIndexOfTerm(conflictingTerm)
		rf.persist()
		return

	}

	// terms match on the two sides
	reply.Success = true
	rf.Logs._AppendEntries(args.PrevLogIndex, args.Entries)
	rf.persist()
	if args.LeaderCommit > rf.CommitIndex {
		newCommitIndex := min(args.LeaderCommit, rf.Logs.LastEntryIndex)
		rf._sendCommandsFromLogs(rf.CommitIndex, newCommitIndex)
		rf.Log(fmt.Sprint("commit index update from ", rf.CommitIndex, " to ", newCommitIndex))
		rf.CommitIndex = newCommitIndex
	}
}

func (rf *Raft) _resetElectionsTimeout() {
	rf.ElectionsTimerStartReference = time.Now().UnixMilli()
}

func (l *Logs) _moveIndexTo(index int) {
	l.LastEntryIndex = index
}

func (l *Logs) _lastEntryTerm() int {
	lastEntryTerm := -1
	if l.LastEntryIndex != -1 {
		lastEntryTerm = l.Logs[l.LastEntryIndex].Term
	}
	return lastEntryTerm
}

func (l *Logs) _termAt(index int) int {
	term := -1
	if index >= 0 {
		term = l.Logs[index].Term
	}
	return term
}

func (l *Logs) _copyStartingFrom(start int) []Log {
	entries := &[]Log{}
	for i := start; i <= l.LastEntryIndex; i++ {
		log := l.Logs[i]
		logCopy := Log{
			Term:    log.Term,
			Command: log.Command,
		}
		*entries = append(*entries, logCopy)
	}

	return *entries
}

func (l *Logs) _AppendEntries(start int, entries []Log) {

	for i := 1; i <= len(entries); i++ {
		l.Logs[i+start].Command = entries[i-1].Command
		l.Logs[i+start].Term = entries[i-1].Term
	}
	l.LastEntryIndex = start + len(entries)
}

func (rf *Raft) Log(message string) {
	// fmt.Println(rf.me, ":", message)
}

func (rf *Raft) _logState() {
	s := fmt.Sprintf("t=%d lei=%d let=%d  ci=%d vf=%d", rf.CurrentTerm, rf.Logs.LastEntryIndex, rf.Logs._lastEntryTerm(), rf.CommitIndex, rf.VotedFor)
	rf.Log(s)
}

func (rf *Raft) _logAndReleaseLock() {
	rf._logState()
	rf.mu.Unlock()
}

func (l *Logs) _firstOccuranceIndexOfTerm(term int) int {
	//should return 0, if term nolonger exist
	index := 0
	for i := 0; i <= l.LastEntryIndex; i++ {
		entry := l.Logs[i]
		if entry.Term == term {
			index = i
			break
		}
	}

	return index
}
