package raft

import (
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
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
	rf.Log("vr from %d(t=%d) ", args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}
	if rf.CurrentTerm == args.Term {
		if rf.VotedFor < 0 || rf.VotedFor == args.CandidateId {
			rf._voteFor(args.CandidateId)
			rf._resetElectionsTimeout()
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
		reply.Term = rf.CurrentTerm
		return
	}
	//rf.CurrentTerm < args.Term
	rf._becomeFollower(args.Term)
	rf._resetElectionsTimeout()
	rf._voteFor(args.CandidateId)
	reply.VoteGranted = true
	reply.Term = rf.CurrentTerm
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RunElections() {

	rf.mu.Lock()

	if rf.Status == STATUS_LEADER {
		rf.mu.Unlock()
		return
	}

	if time.Now().Before(rf.ElectionTimeout) {
		rf.mu.Unlock()
		return
	}

	rf._promoteToCandidate()
	term := rf.CurrentTerm
	candidateId := rf.me
	deadline := rf.ElectionTimeout

	rf.mu.Unlock()
	defer func(term int) { rf.Log(" done with election t = %d", term) }(term)

	rf.Log("running vote")

	votes := 1
	rejections := 0
	repCh := make(chan RequestVoteReply, len(rf.peers)-1)

	args := RequestVoteArgs{
		Term:        term,
		CandidateId: candidateId,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == candidateId {
			continue
		}
		go func(serverId int) {
			reply := RequestVoteReply{}
			rf.Log("RV call start -> %d t=%d", serverId, term)
			ok := rf.sendRequestVote(serverId, &args, &reply)
			rf.Log("RV call done -> %d (ok? %v)", serverId, ok)
			if ok {
				repCh <- reply
			}
		}(i)
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return
		case reply := <-repCh:
			rf.mu.Lock()

			if rf.Status != STATUS_CANDIDATE || rf.CurrentTerm != term {
				rf.mu.Unlock()
				break
			}

			if reply.Term > term {
				rf._becomeFollower(reply.Term)
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				votes++
			} else {
				rejections++
			}

			if rf.IsMajority(votes) {
				rf._promoteToLeader()
				rf.mu.Unlock()
				return
			}

			if rf.IsMajority(rejections) {
				rf._resetElectionsTimeout()
				rf.mu.Unlock()
				return
			}

			rf.mu.Unlock()
		}
	}
}
